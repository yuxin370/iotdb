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

package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;
import org.apache.iotdb.tsfile.utils.RLEPattern;

import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;

public class RLEColumnBuilder implements ColumnBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(RLEColumnBuilder.class);

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(IntColumnBuilder.class).instanceSize();

  private final ColumnBuilderStatus columnBuilderStatus;
  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private int patternCount; // count of valid RlePatterns
  private TSDataType dataType;
  private boolean hasNonNullValue;

  // it is assumed that patternOffsetIndex.length = values.length + 1
  private RLEPattern[] values = new RLEPattern[0];
  private int[] patternOffsetIndex =
      new int[] {
        0
      }; // patternOffsetIndex[i] refers to the offset of values[i].getObject(0) in all data.

  private long retainedSizeInBytes;

  public RLEColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries, TSDataType type) {
    this.columnBuilderStatus = columnBuilderStatus;
    this.initialEntryCount = max(expectedEntries, 1);
    this.dataType = type;

    updateDataSize();
  }

  public RLEColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries, TypeEnum type) {
    this.columnBuilderStatus = columnBuilderStatus;
    this.initialEntryCount = max(expectedEntries, 1);
    switch (type) {
      case INT32:
        this.dataType = TSDataType.INT32;
        break;
      case INT64:
        this.dataType = TSDataType.INT64;
        break;
      case FLOAT:
        this.dataType = TSDataType.FLOAT;
        break;
      case DOUBLE:
        this.dataType = TSDataType.DOUBLE;
        break;
      case BOOLEAN:
        this.dataType = TSDataType.BOOLEAN;
        break;
      case BINARY:
        this.dataType = TSDataType.TEXT;
        break;
      default:
        throw new UnSupportedDataTypeException("RLEColumn builder do not support " + type);
    }
    updateDataSize();
  }

  @Override
  public ColumnBuilder writeColumn(Column value, int logicPositionCount) {
    if (!value.getDataType().equals(dataType)) {
      throw new UnSupportedDataTypeException(
          " only " + dataType + " supported, but get " + value.getDataType());
    }

    if (values.length <= patternCount) {
      growCapacity();
    }
    RLEPattern pattern = new RLEPattern(value, logicPositionCount);
    values[patternCount] = pattern;
    patternOffsetIndex[patternCount + 1] = patternOffsetIndex[patternCount] + logicPositionCount;
    hasNonNullValue = true;
    patternCount++;
    positionCount += logicPositionCount;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes((int) value.getRetainedSizeInBytes());
    }
    return this;
  }

  @Override
  public ColumnBuilder write(Column column, int index) {
    return writeColumn(column.getColumn(index), ((RLEColumn) column).getLogicPositionCount(index));
  }

  @Override
  public ColumnBuilder appendNull() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      switch (getDataType()) {
        case INT32:
          return new RunLengthEncodedColumn(
              new IntColumn(0, 1, new boolean[] {true}, new int[1]), positionCount);
        case INT64:
          return new RunLengthEncodedColumn(
              new LongColumn(0, 1, new boolean[] {true}, new long[1]), positionCount);
        case FLOAT:
          return new RunLengthEncodedColumn(
              new FloatColumn(0, 1, new boolean[] {true}, new float[1]), positionCount);
        case DOUBLE:
          return new RunLengthEncodedColumn(
              new DoubleColumn(0, 1, new boolean[] {true}, new double[1]), positionCount);
        case BOOLEAN:
          return new RunLengthEncodedColumn(
              new BooleanColumn(0, 1, new boolean[] {true}, new boolean[1]), positionCount);
        default:
          throw new UnSupportedDataTypeException(
              "Unsupported DataType for RLEColumn:" + getDataType());
      }
    }
    return new RLEColumn(positionCount, patternCount, values, patternOffsetIndex);
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    return new RLEColumnBuilder(
        columnBuilderStatus, calculateBlockResetSize(positionCount), dataType);
  }

  private void growCapacity() {
    int newSize;
    if (initialized) {
      newSize = ColumnUtil.calculateNewArraySize(values.length);
    } else {
      newSize = initialEntryCount;
      initialized = true;
    }

    patternOffsetIndex = Arrays.copyOf(patternOffsetIndex, newSize + 1);
    values = Arrays.copyOf(values, newSize);
    updateDataSize();
  }

  private void updateDataSize() {
    retainedSizeInBytes = INSTANCE_SIZE + sizeOf(patternOffsetIndex) + sizeOf(values);
    if (columnBuilderStatus != null) {
      retainedSizeInBytes += ColumnBuilderStatus.INSTANCE_SIZE;
    }
  }
}
