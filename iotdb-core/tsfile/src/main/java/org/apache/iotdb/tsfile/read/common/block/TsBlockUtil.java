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

package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.util.List;

public class TsBlockUtil {

  private TsBlockUtil() {
    // forbidding instantiation
  }

  /** Skip lines at the beginning of the tsBlock that are not in the time range. */
  public static TsBlock skipPointsOutOfTimeRange(
      TsBlock tsBlock, TimeRange targetTimeRange, boolean ascending) {
    int firstIndex = getFirstConditionIndex(tsBlock, targetTimeRange, ascending);
    return tsBlock.subTsBlock(firstIndex);
  }

  // If ascending, find the index of first greater than or equal to targetTime
  // else, find the index of first less than or equal to targetTime
  public static int getFirstConditionIndex(
      TsBlock tsBlock, TimeRange targetTimeRange, boolean ascending) {
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? targetTimeRange.getMin() : targetTimeRange.getMax();
    int left = 0;
    int right = timeColumn.getPositionCount() - 1;
    int mid;

    while (left < right) {
      mid = (left + right) >> 1;
      if (timeColumn.getLongWithoutCheck(mid) < targetTime) {
        if (ascending) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) > targetTime) {
        if (ascending) {
          right = mid;
        } else {
          left = mid + 1;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) == targetTime) {
        return mid;
      }
    }
    return left;
  }

  // convert RLEColumn to generic column
  public static Column convertRLEColumnToGenericColumn(Column column) {
    if (!(column instanceof RLEColumn)) {
      return column;
    }
    TSDataType dataType = column.getDataType();
    int positionCount = column.getPositionCount();
    switch (dataType) {
      case INT32:
        IntColumnBuilder intColumnBuilder = new IntColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              intColumnBuilder.writeInt(column.getInt(i));
            } else {
              intColumnBuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            intColumnBuilder.writeInt(column.getInt(i));
          }
        }
        return intColumnBuilder.build();
      case BOOLEAN:
        BooleanColumnBuilder booleanColumnbuilder = new BooleanColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              booleanColumnbuilder.writeBoolean(column.getBoolean(i));
            } else {
              booleanColumnbuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            booleanColumnbuilder.writeBoolean(column.getBoolean(i));
          }
        }
        return booleanColumnbuilder.build();
      case DOUBLE:
        DoubleColumnBuilder doubleColumnBuilder = new DoubleColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              doubleColumnBuilder.writeDouble(column.getDouble(i));
            } else {
              doubleColumnBuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            doubleColumnBuilder.writeDouble(column.getDouble(i));
          }
        }
        return doubleColumnBuilder.build();
      case FLOAT:
        FloatColumnBuilder floatColumnBuilder = new FloatColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              floatColumnBuilder.writeFloat(column.getFloat(i));
            } else {
              floatColumnBuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            floatColumnBuilder.writeFloat(column.getFloat(i));
          }
        }
        return floatColumnBuilder.build();
      case INT64:
        LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              longColumnBuilder.writeLong(column.getLong(i));
            } else {
              longColumnBuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            longColumnBuilder.writeLong(column.getLong(i));
          }
        }
        return longColumnBuilder.build();
      case TEXT:
        BinaryColumnBuilder binaryColumnBuilder = new BinaryColumnBuilder(null, positionCount);
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (column.isNull(i)) {
              binaryColumnBuilder.writeFloat(column.getFloat(i));
            } else {
              binaryColumnBuilder.appendNull();
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            binaryColumnBuilder.writeFloat(column.getFloat(i));
          }
        }
        return binaryColumnBuilder.build();
      default:
        throw new UnSupportedDataTypeException(
            "RLEColumn can't be converted to " + dataType + " column.");
    }
  }

  public static ColumnBuilder[] contructColumnBuilder(List<TSDataType> dataType) {
    ColumnBuilder[] valueColumnBuilders = new ColumnBuilder[dataType.size()];
    for (int i = 0; i < dataType.size(); i++) {
      switch (dataType.get(i)) {
        case BOOLEAN:
          valueColumnBuilders[i] = new BooleanColumnBuilder(null, 0);
          break;
        case INT32:
          valueColumnBuilders[i] = new IntColumnBuilder(null, 0);
          break;
        case INT64:
          valueColumnBuilders[i] = new LongColumnBuilder(null, 0);
          break;
        case FLOAT:
          valueColumnBuilders[i] = new FloatColumnBuilder(null, 0);
          break;
        case DOUBLE:
          valueColumnBuilders[i] = new DoubleColumnBuilder(null, 0);
          break;
        case TEXT:
          valueColumnBuilders[i] = new BinaryColumnBuilder(null, 0);
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + dataType.get(i));
      }
    }
    return valueColumnBuilders;
  }
}
