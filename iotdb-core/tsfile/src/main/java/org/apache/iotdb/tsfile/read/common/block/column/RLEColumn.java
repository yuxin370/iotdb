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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

// a column store values in RLEPattern.
// attention: all the functions overwrited, use index indexing the raw data.
// all the functions named "*RLE*", use index indexing to the RLEPatterns.
public class RLEColumn implements Column {
  private static final Logger LOGGER = LoggerFactory.getLogger(RLEColumn.class);

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(RunLengthEncodedColumn.class).instanceSize();

  private final int arrayOffset;
  private final int positionCount;
  private final boolean[] valueIsNull;
  private final RLEPatternColumn[] values;

  public RLEColumn(int positionCount, Optional<boolean[]> valueIsNull, Column[] values) {
    this(0, positionCount, valueIsNull.orElse(null), values);
  }

  RLEColumn(int arrayOffset, int positionCount, boolean[] valueIsNull, Column[] values) {
    requireNonNull(values, "values is null");
    if (arrayOffset < 0) {
      throw new IllegalArgumentException("arrayOffset is negative");
    }
    this.arrayOffset = arrayOffset;
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;

    if (values.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("values length is less than positionCount");
    }

    // if (!(values[0] instanceof RLEPatternColumn)) {
    //   throw new IllegalArgumentException("column must be RLEPattern Columns" +
    // values[0].getClass());
    // }

    this.values = (RLEPatternColumn[]) values;

    if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("isNull length is less than positionCount");
    }
    this.valueIsNull = valueIsNull;
  }

  public RLEPatternColumn getRLEPattern(int position) {
    if (arrayOffset + position > positionCount) {
      throw new IllegalArgumentException("position is illegal");
    }
    return values[arrayOffset + position];
  }

  public RLEPatternColumn[] getValues() {
    return values;
  }

  public Object getValue(int position) {
    // position correspoding to the index in raw data
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[index].getPositionCount() >= position) {
        break;
      }
      accumulator += values[index].getPositionCount();
    }
    if (arrayOffset + index > positionCount) {
      throw new IllegalArgumentException("position is illegal");
    }
    return values[arrayOffset + index].getObject(position - accumulator);
  }

  @Override
  public Object getObject(int position) {
    return getValue(position);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.RLEPATTERN;
  }

  /** get dataType of the actual stored values. */
  public TSDataType getValueDataType() {
    if (valueIsNull == null) {
      return values[arrayOffset].getDataType();
    } else {
      int i;
      for (i = arrayOffset; i < arrayOffset + positionCount && valueIsNull[i] == true; i++) ;
      if (i < arrayOffset + positionCount) {
        return values[arrayOffset + i].getDataType();
      } else {
        return TSDataType.UNKNOWN;
      }
    }
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.RLE_ARRAY;
  }

  @Override
  public Object[] getObjects() {
    return values;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    // position correspoding to the index in raw data
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[index].getPositionCount() >= position) {
        break;
      }
      accumulator += values[index].getPositionCount();
    }

    return values[index].getTsPrimitiveType(position - accumulator);
  }

  public TsPrimitiveType getTsPrimitiveTypeRLE(int position) {
    RLEPatternColumn curRLEPatternColumn = getRLEPattern(position);
    int Mode = curRLEPatternColumn.isRLEMode() ? 0 : 1;
    int RLEPatternCount = curRLEPatternColumn.getPositionCount();
    TsPrimitiveType[] values;
    if (Mode == 1) {
      values = new TsPrimitiveType[RLEPatternCount];
      for (int i = 0; i < RLEPatternCount; i++) {
        values[i] = curRLEPatternColumn.getTsPrimitiveType(i);
      }
    } else {
      values = new TsPrimitiveType[] {curRLEPatternColumn.getTsPrimitiveType(0)};
    }
    return new TsPrimitiveType.TsRLEPattern(values, Mode, RLEPatternCount);
  }

  @Override
  public boolean mayHaveNull() {
    return valueIsNull != null;
  }

  @Override
  public boolean isNull(int position) {
    // position correspoding to the index in raw data
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[index].getPositionCount() >= position) {
        break;
      }
      accumulator += values[index].getPositionCount();
    }
    return (valueIsNull != null && valueIsNull[index + arrayOffset])
        || (values[index + arrayOffset].isNull(position - accumulator));
  }

  public boolean isNullRLE(int position) {
    return valueIsNull != null && valueIsNull[position + arrayOffset];
  }

  @Override
  public boolean[] isNull() {
    if (valueIsNull == null) {
      boolean[] res = new boolean[positionCount];
      Arrays.fill(res, false);
      return res;
    }
    return valueIsNull;
  }

  /** get positionCount, which is the number of the RLEPatternColumn */
  @Override
  public int getPositionCount() {
    return positionCount;
  }

  /** get the number of the actual values, which eauqls to sum(RLEPatternCoumns[i].PositionCount) */
  public int getValueCount() {
    int valueCount = 0;
    if (valueIsNull == null) {
      for (int i = 0; i < positionCount; i++) {
        valueCount += values[i].getPositionCount();
      }
    } else {
      if (valueIsNull == null) {
        for (int i = 0; i < positionCount; i++) {
          if (valueIsNull[i] != true) {
            valueCount += values[i].getPositionCount();
          }
        }
      }
    }
    return valueCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    long retainedSizeInBytes = 0;
    if (valueIsNull != null) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueIsNull[arrayOffset + i]) {
          retainedSizeInBytes += values[arrayOffset + i].getRetainedSizeInBytes();
        }
      }
    } else {
      for (int i = 0; i < positionCount; i++) {
        retainedSizeInBytes += values[arrayOffset + i].getRetainedSizeInBytes();
      }
    }
    retainedSizeInBytes += INSTANCE_SIZE;
    return retainedSizeInBytes;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RLEColumn(arrayOffset + positionOffset, length, valueIsNull, values);
  }

  @Override
  public Column subColumn(int fromIndex) {
    // position correspoding to the index in raw data
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[index].getPositionCount() >= fromIndex) {
        break;
      }
      accumulator += values[index].getPositionCount();
    }
    if (index > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    if (accumulator == fromIndex) {
      return new RLEColumn(arrayOffset + index, positionCount - index, valueIsNull, values);
    } else {
      /*reconstruct the values  */
      RLEPatternColumn[] tmpValues = new RLEPatternColumn[positionCount];
      tmpValues = Arrays.copyOf(values, positionCount);
      tmpValues[index] = (RLEPatternColumn) tmpValues[index].subColumn(fromIndex - accumulator);
      return new RLEColumn(arrayOffset + index, positionCount - index, valueIsNull, tmpValues);
    }
  }

  public Column subColumnRLE(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new RLEColumn(arrayOffset + fromIndex, positionCount - fromIndex, valueIsNull, values);
  }

  @Override
  public Column subColumn(boolean[] valueRetained) {
    if (valueRetained.length != positionCount) {
      throw new IllegalArgumentException("valueRetained is not valid");
    }
    int newCount = 0;
    for (int i = 0; i < positionCount; i++) {
      if (valueRetained[i] == true) {
        newCount++;
      }
    }
    if (newCount == positionCount) {
      return new RLEColumn(0, newCount, valueIsNull, values);
    }
    RLEPatternColumn[] newValue = new RLEPatternColumn[newCount];
    boolean[] newValueIsNull = new boolean[newCount];
    if (valueIsNull == null) {
      newValueIsNull = null;
      for (int i = 0, j = 0; i < positionCount; i++) {
        if (valueRetained[i] == true) {
          newValue[j] = values[j];
          j++;
        }
      }
    } else {
      for (int i = 0, j = 0; i < positionCount; i++) {
        if (valueRetained[i] == true) {
          newValue[j] = values[j];
          newValueIsNull[j] = valueIsNull[j];
          j++;
        }
      }
    }
    return new RLEColumn(0, newCount, newValueIsNull, newValue);
  }

  @Override
  public void reverse() {
    for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
      RLEPatternColumn valueTmp = values[i];
      values[i] = values[j];
      values[j] = valueTmp;
      values[i].reverse();
      values[j].reverse();
    }
    if (valueIsNull != null) {
      for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
        boolean isNullTmp = valueIsNull[i];
        valueIsNull[i] = valueIsNull[j];
        valueIsNull[j] = isNullTmp;
      }
    }
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  // position correspoding to the index in raw data
  public void updateValue(int position, Object value) {
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[index].getPositionCount() >= position) {
        break;
      }
      accumulator += values[index].getPositionCount();
    }
    values[index].getObjects()[position - accumulator] = value;
  }
}
