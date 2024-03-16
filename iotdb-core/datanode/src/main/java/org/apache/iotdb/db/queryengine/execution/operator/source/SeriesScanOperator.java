/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SeriesScanOperator extends AbstractDataSourceOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SeriesScanOperator.class);

  private boolean finished = false;

  public SeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          break;
        }
      } while (System.nanoTime() - start < maxRuntime && !resultTsBlockBuilder.isFull());

      finished = resultTsBlockBuilder.isEmpty();

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemory() - calculateMaxReturnSize();
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();

      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendRLEToValueBuilder(TsBlock tsBlock) {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);
    RLEColumn column = (RLEColumn) tsBlock.getColumn(0);
    int countFlag = 0;
    if (column.mayHaveNull()) {
      for (int i = 0, size = column.getPositionCount(); i < size; i++) {
        int patternLength = 1;
        if (column.isNullRLE(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeRLEPattern(column, i);
          patternLength = ((RLEPatternColumn) column.getRLEPattern(i)).getPositionCount();
        }
        resultTsBlockBuilder.declarePositions(patternLength);
        for (int c = countFlag; c < countFlag + patternLength; c++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(c));
        }
        countFlag += patternLength;
      }
    } else {
      for (int i = 0, size = column.getPositionCount(); i < size; i++) {
        columnBuilder.writeRLEPattern(column, i);
        int patternLength = ((RLEPatternColumn) column.getRLEPattern(i)).getPositionCount();
        resultTsBlockBuilder.declarePositions(patternLength);
        for (int c = countFlag; c < countFlag + patternLength; c++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(c));
        }
        countFlag += patternLength;
      }
    }
  }

  private void appendRLEToBuilder(TsBlock tsBlock) {
    if (resultTsBlockBuilder.getType(0) != TSDataType.RLEPATTERN) {
      resultTsBlockBuilder.buildValueColumnBuilders(
          Collections.singletonList(TSDataType.RLEPATTERN));
    }
    /**
     * 1、是否遇到RLE
     * 2、是否 builder 中有数据
     * 3、builder是否是RLEbuilder
     * 
     * 1）有数据 且 RLEBuilder + RLE ->  正常写
     * 2) 有数据 非 RLEBuilder + RLE  -> 正常写
     * 3) 无数据 非 RLEBuilder + RLE  -> 创建 替换 RLEBuilder
     * 4）有数据 且 RLEBuilder + single value  -> 整个column塞进去 = 正常写
     * 5) 有数据 非 RLEBuilder + single value  -> 正常写
     * 6) 无数据 非 RLEBuilder + single value  -> 正常写
     */
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    RLEColumnBuilder columnBuilder = (RLEColumnBuilder) resultTsBlockBuilder.getColumnBuilder(0);
    RLEColumn column = (RLEColumn) tsBlock.getColumn(0);
    int countFlag = 0;
    if (column.mayHaveNull()) {
      for (int i = 0, size = column.getPositionCount(); i < size; i++) {
        int patternLength = 1;
        if (column.isNullRLE(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeRLEPattern(column, i);
          patternLength = ((RLEPatternColumn) column.getRLEPattern(i)).getPositionCount();
        }
        resultTsBlockBuilder.declarePositions(patternLength);
        for (int c = countFlag; c < countFlag + patternLength; c++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(c));
        }
        countFlag += patternLength;
      }
    } else {
      for (int i = 0, size = column.getPositionCount(); i < size; i++) {
        columnBuilder.writeRLEPattern(column, i);
        int patternLength = ((RLEPatternColumn) column.getRLEPattern(i)).getPositionCount();
        resultTsBlockBuilder.declarePositions(patternLength);
        for (int c = countFlag; c < countFlag + patternLength; c++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(c));
        }
        countFlag += patternLength;
      }
    }
  }

  private void appendToBuilder(TsBlock tsBlock) {
    Column column = tsBlock.getColumn(0);
    if (column instanceof RLEColumn) {
      appendRLEToBuilder(tsBlock);
    } else {
      TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
      TimeColumn timeColumn = tsBlock.getTimeColumn();
      ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);
      if (column.mayHaveNull()) {
        for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(i));
          if (column.isNull(i)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(column, i);
          }
          resultTsBlockBuilder.declarePosition();
        }
      } else {
        for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
          timeColumnBuilder.writeLong(timeColumn.getLong(i));
          columnBuilder.write(column, i);
          resultTsBlockBuilder.declarePosition();
        }
      }
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return seriesScanUtil.getTsDataTypeList();
  }
}
