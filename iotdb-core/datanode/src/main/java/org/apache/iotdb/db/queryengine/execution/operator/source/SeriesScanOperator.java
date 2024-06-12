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
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RLEColumn;
import org.apache.tsfile.read.common.block.column.RLEColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SeriesScanOperator extends AbstractDataSourceOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SeriesScanOperator.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SeriesScanOperator.class);

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
    return Math.max(
        maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() * 3L);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
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

  private void appendRLEToBuilder(TsBlock tsBlock) {
    RLEColumn column = (RLEColumn) tsBlock.getColumn(0);
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);
    Column storeColumn = null;
    if (!(columnBuilder instanceof RLEColumnBuilder)) {
      if (resultTsBlockBuilder.getPositionCount() == 0) {
        resultTsBlockBuilder.buildValueColumnBuilders(
            new ColumnBuilder[] {new RLEColumnBuilder(null, 1, columnBuilder.getDataType())});
      } else {
        storeColumn = columnBuilder.build();
        resultTsBlockBuilder.buildValueColumnBuilders(
            new ColumnBuilder[] {new RLEColumnBuilder(null, 1, columnBuilder.getDataType())});
      }
    }
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    RLEColumnBuilder rlecolumnBuilder = (RLEColumnBuilder) resultTsBlockBuilder.getColumnBuilder(0);
    if (storeColumn != null) {
      rlecolumnBuilder.writeRLEPattern(storeColumn, storeColumn.getPositionCount());
    }
    Pair<Column[], int[]> patterns = column.getVisibleColumns();
    Column[] columns = patterns.getLeft();
    int[] logicPositionCounts = patterns.getRight();
    int rowFlag = 0;

    for (int i = 0, size = columns.length; i < size; i++) {
      rlecolumnBuilder.writeRLEPattern(columns[i], logicPositionCounts[i]);
      resultTsBlockBuilder.declarePositions(logicPositionCounts[i]);
      for (int c = rowFlag; c < rowFlag + logicPositionCounts[i]; c++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(c));
      }
      rowFlag += logicPositionCounts[i];
    }
  }

  private void appendToBuilder(TsBlock tsBlock) {
    /**
     * Check the following conditions: 1) Whether it encounters RLE (Run-Length Encoding). 2)
     * Whether there is data in the builder. 3) Whether the builder is an RLE builder.
     *
     * <p>Cases: 1) If there is data and the builder is an RLEBuilder, and RLE is encountered: Write
     * normally. 2) If there is data and the builder is not an RLEBuilder, and RLE is encountered:
     * Write normally. 3) If there is no data and the builder is not an RLEBuilder, and RLE is
     * encountered: Replace the existing Builder with a RLEbuilder. 4) If there is data and the
     * builder is an RLEBuilder, and a single value is encountered: Write the entire column to
     * RLEBuilde. 5) If there is data and the builder is not an RLEBuilder, and a single value is
     * encountered: Write normally. 6) If there is no data and the builder is not an RLEBuilder, and
     * a single value is encountered: Write normally.
     */
    Column column = tsBlock.getColumn(0);
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);
    if ((column instanceof RLEColumn)) {
      appendRLEToBuilder(tsBlock);
    } else if (columnBuilder instanceof RLEColumnBuilder) {
      TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
      TimeColumn timeColumn = tsBlock.getTimeColumn();
      ((RLEColumnBuilder) columnBuilder).writeRLEPattern(column, column.getPositionCount());
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
      }
      resultTsBlockBuilder.declarePositions(tsBlock.getPositionCount());
    } else {
      TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
      TimeColumn timeColumn = tsBlock.getTimeColumn();
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

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes());
  }
}
