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
import org.apache.iotdb.tsfile.utils.Binary;
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
// all the functions named "*RLE*", use index indexing the RLEPatterns.
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

    this.values = (RLEPatternColumn[]) values;

    if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("isNull length is less than positionCount");
    }
    this.valueIsNull = valueIsNull;
  }

  /** TODO */
  /** reclaim RLEColumn to corresponding raw dataType Column */
  public Object reclaim() {
    TSDataType valueDataType = getValueDataType();
    int totalValueCount = getValueCount();
    int index = 0;
    switch (valueDataType) {
      case INT32:
        int[] intValues = new int[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            int[] curValue = values[arrayOffset + i].getInts();
            System.arraycopy(curValue, 0, intValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              int[] curValue = values[arrayOffset + i].getInts();
              System.arraycopy(curValue, 0, intValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new IntColumn(totalValueCount, Optional.empty(), intValues));
      case BOOLEAN:
        boolean[] booleanValues = new boolean[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            boolean[] curValue = values[arrayOffset + i].getBooleans();
            System.arraycopy(curValue, 0, booleanValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              boolean[] curValue = values[arrayOffset + i].getBooleans();
              System.arraycopy(curValue, 0, booleanValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new BooleanColumn(totalValueCount, Optional.empty(), booleanValues));
      case DOUBLE:
        double[] doubleValues = new double[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            double[] curValue = values[arrayOffset + i].getDoubles();
            System.arraycopy(curValue, 0, doubleValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              double[] curValue = values[arrayOffset + i].getDoubles();
              System.arraycopy(curValue, 0, doubleValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new DoubleColumn(totalValueCount, Optional.empty(), doubleValues));
      case FLOAT:
        float[] floatValues = new float[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            float[] curValue = values[arrayOffset + i].getFloats();
            System.arraycopy(curValue, 0, floatValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              float[] curValue = values[arrayOffset + i].getFloats();
              System.arraycopy(curValue, 0, floatValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new FloatColumn(totalValueCount, Optional.empty(), floatValues));
      case INT64:
        long[] longValues = new long[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            long[] curValue = values[arrayOffset + i].getLongs();
            System.arraycopy(curValue, 0, longValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              long[] curValue = values[arrayOffset + i].getLongs();
              System.arraycopy(curValue, 0, longValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new LongColumn(totalValueCount, Optional.empty(), longValues));
      case TEXT:
        Binary[] textValues = new Binary[totalValueCount];
        if (valueIsNull == null) {
          for (int i = 0; i < positionCount; i++) {
            int curCount = values[arrayOffset + i].getPositionCount();
            Binary[] curValue = values[arrayOffset + i].getBinaries();
            System.arraycopy(curValue, 0, textValues, index, curCount);
            index += curCount;
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[arrayOffset + i] != true) {
              int curCount = values[arrayOffset + i].getPositionCount();
              Binary[] curValue = values[arrayOffset + i].getBinaries();
              System.arraycopy(curValue, 0, textValues, index, curCount);
              index += curCount;
            }
          }
        }
        return (Object) (new BinaryColumn(totalValueCount, Optional.empty(), textValues));
      default:
        throw new UnSupportedDataTypeException(
            "RLEColumn can't be reclaimed to " + valueDataType + " column.");
    }
  }

  public RLEPatternColumn getRLEPattern(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + position + " out of the bound of positionCount: " + positionCount);
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
      if (accumulator + values[arrayOffset + index].getPositionCount() > position) {
        break;
      }
      accumulator += values[arrayOffset + index].getPositionCount();
    }
    if (index >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + position + " out of the bound of positionCount: " + positionCount);
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
        return values[i].getDataType();
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
      if (accumulator + values[arrayOffset + index].getPositionCount() > position) {
        break;
      }
      accumulator += values[arrayOffset + index].getPositionCount();
    }

    return values[arrayOffset + index].getTsPrimitiveType(position - accumulator);
  }

  public TsPrimitiveType getTsPrimitiveTypeRLE(int position) {
    RLEPatternColumn curRLEPatternColumn = getRLEPattern(position);
    int Mode = curRLEPatternColumn.isRLEMode() ? 0 : 1;
    int RLEPatternCount = curRLEPatternColumn.getPositionCount();
    TsPrimitiveType[] values;
    if (Mode == 1) {
      values = new TsPrimitiveType[RLEPatternCount];
      for (int i = 0; i < RLEPatternCount; i++) {
        values[arrayOffset + i] = curRLEPatternColumn.getTsPrimitiveType(i);
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
      if (accumulator + values[arrayOffset + index].getPositionCount() > position) {
        break;
      }
      accumulator += values[arrayOffset + index].getPositionCount();
    }
    return (valueIsNull != null && valueIsNull[index + arrayOffset])
        || (values[index + arrayOffset].isNull(position - accumulator));
  }

  @Override
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
        valueCount += values[arrayOffset + i].getPositionCount();
      }
    } else {
      for (int i = 0; i < positionCount; i++) {
        if (valueIsNull[arrayOffset + i] != true) {
          valueCount += values[arrayOffset + i].getPositionCount();
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
    checkValidRegion(getValueCount(), positionOffset, length);
    // position correspoding to the index in raw data
    int index = 0;
    int offsetAccumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (offsetAccumulator + values[arrayOffset + index].getPositionCount() > positionOffset) {
        break;
      }
      offsetAccumulator += values[arrayOffset + index].getPositionCount();
    }
    int lengthAccumulator = offsetAccumulator;
    int lengthIndex;
    int lengthEnd = positionOffset + length;
    for (lengthIndex = index; lengthIndex < this.positionCount; lengthIndex++) {
      if (lengthAccumulator + values[arrayOffset + lengthIndex].getPositionCount() >= lengthEnd) {
        break;
      }
      lengthAccumulator += values[arrayOffset + lengthIndex].getPositionCount();
    }
    if (index >= positionCount || lengthIndex >= positionCount) {
      throw new IllegalArgumentException("index or length is not valid");
    }
    /*reconstruct the values  */
    RLEPatternColumn[] tmpValues = new RLEPatternColumn[positionCount];
    tmpValues = Arrays.copyOf(values, positionCount);
    tmpValues[arrayOffset + index] =
        (RLEPatternColumn)
            tmpValues[arrayOffset + index].subColumn(positionOffset - offsetAccumulator);
    tmpValues[arrayOffset + lengthIndex] =
        (RLEPatternColumn)
            tmpValues[arrayOffset + lengthIndex].subColumnHead(lengthEnd - lengthAccumulator);

    return new RLEColumn(arrayOffset + index, lengthIndex - index + 1, valueIsNull, tmpValues);
  }

  public Column getRegionRLE(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RLEColumn(arrayOffset + positionOffset, length, valueIsNull, values);
  }

  @Override
  public Column subColumn(int fromIndex) {
    // position correspoding to the index in raw data
    int index = 0;
    int accumulator = 0;
    for (index = 0; index < this.positionCount; index++) {
      if (accumulator + values[arrayOffset + index].getPositionCount() > fromIndex) {
        break;
      }
      accumulator += values[arrayOffset + index].getPositionCount();
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
      tmpValues[arrayOffset + index] =
          (RLEPatternColumn) tmpValues[arrayOffset + index].subColumn(fromIndex - accumulator);
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
          newValue[j] = values[arrayOffset + i];
          j++;
        }
      }
    } else {
      for (int i = 0, j = 0; i < positionCount; i++) {
        if (valueRetained[i] == true) {
          newValue[j] = values[arrayOffset + i];
          newValueIsNull[j] = valueIsNull[arrayOffset + i];
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
      if (accumulator + values[arrayOffset + index].getPositionCount() > position) {
        break;
      }
      accumulator += values[arrayOffset + index].getPositionCount();
    }
    values[arrayOffset + index].getObjects()[position - accumulator] = value;
  }
}
