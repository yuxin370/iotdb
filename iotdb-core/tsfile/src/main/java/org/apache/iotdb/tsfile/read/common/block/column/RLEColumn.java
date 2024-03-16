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

  private static final int INSTANCE_SIZE = ClassLayout.parseClass(RLEColumn.class).instanceSize();

  private final int arrayOffset;
  private final int positionCount;
  private final boolean[] valueIsNull;
  private final RLEPatternColumn[] values;
  private int[] patternOffsetIndex;

  public RLEColumn(int positionCount, Optional<boolean[]> valueIsNull, Column[] values) {
    this(0, positionCount, valueIsNull.orElse(null), null, values);
  }

  public RLEColumn(
      int arrayOffset, int positionCount, Optional<boolean[]> valueIsNull, Column[] values) {
    this(arrayOffset, positionCount, valueIsNull.orElse(null), null, values);
  }

  public RLEColumn(
      int positionCount,
      Optional<boolean[]> valueIsNull,
      int[] patternOffsetIndex,
      Column[] values) {
    this(0, positionCount, valueIsNull.orElse(null), patternOffsetIndex, values);
  }

  RLEColumn(
      int arrayOffset,
      int positionCount,
      boolean[] valueIsNull,
      int[] patternOffsetIndex,
      Column[] values) {
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

    if (patternOffsetIndex != null && patternOffsetIndex.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("patternOffsetIndex length is less than positionCount");
    }
    this.patternOffsetIndex = patternOffsetIndex;
  }

  private void constructPatternOffsetIndex() {
    this.patternOffsetIndex = new int[positionCount];
    this.patternOffsetIndex[0] =
        valueIsNull != null && valueIsNull[0 + arrayOffset]
            ? 0
            : this.values[0 + arrayOffset].getPositionCount();
    if (valueIsNull == null) {
      for (int i = 1; i < positionCount; i++) {
        this.patternOffsetIndex[i] =
            this.patternOffsetIndex[i - 1] + this.values[arrayOffset + i].getPositionCount();
      }
    } else {
      for (int i = 1; i < positionCount; i++) {
        if (!valueIsNull[arrayOffset + i]) {
          this.patternOffsetIndex[i] =
              this.patternOffsetIndex[i - 1] + this.values[arrayOffset + i].getPositionCount();
        } else {
          this.patternOffsetIndex[i] = this.patternOffsetIndex[i - 1];
        }
      }
    }
  }

  private int getPatternOffsetIndex(int position) {
    if (position == -1) {
      return 0;
    }

    if (position >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + position + " out of the bound of positionCount: " + positionCount);
    }

    if (this.patternOffsetIndex == null) {
      constructPatternOffsetIndex();
    }

    return patternOffsetIndex[position];
  }

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

    for (index = 0; index < this.positionCount && getPatternOffsetIndex(index) <= position; index++)
      ;
    if (index >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + index + " out of the bound of positionCount: " + positionCount);
    }

    return values[arrayOffset + index].getObject(position - getPatternOffsetIndex(index - 1));
    // return values[arrayOffset + index].getObject(position - accumulator);
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

    for (index = 0; index < this.positionCount && getPatternOffsetIndex(index) <= position; index++)
      ;
    if (index >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + index + " out of the bound of positionCount: " + positionCount);
    }

    return values[arrayOffset + index].getTsPrimitiveType(
        position - getPatternOffsetIndex(index - 1));
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
    for (index = 0; index < this.positionCount && getPatternOffsetIndex(index) <= position; index++)
      ;
    if (index >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + index + " out of the bound of positionCount: " + positionCount);
    }
    return (valueIsNull != null && valueIsNull[index + arrayOffset])
        || (values[index + arrayOffset].isNull(position - getPatternOffsetIndex(index - 1)));
    // return (valueIsNull != null && valueIsNull[index + arrayOffset])
    //     || (values[index + arrayOffset].isNull(position - accumulator));
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
    return getPatternOffsetIndex(positionCount - 1);
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
    int startIndex = 0;
    int endIndex = 0;
    int endPosition = positionOffset + length;
    for (startIndex = 0;
        startIndex < this.positionCount && getPatternOffsetIndex(startIndex) <= positionOffset;
        startIndex++) ;
    for (endIndex = startIndex;
        endIndex < this.positionCount && getPatternOffsetIndex(endIndex) < endPosition;
        endIndex++) ;

    if (startIndex >= positionCount || endIndex >= positionCount) {
      throw new IllegalArgumentException("positionOffset or length is not valid");
    }
    /*reconstruct the values  */
    RLEPatternColumn[] tmpValues = new RLEPatternColumn[values.length];
    tmpValues = Arrays.copyOf(values, values.length);
    tmpValues[arrayOffset + startIndex] =
        (RLEPatternColumn)
            tmpValues[arrayOffset + startIndex].subColumn(
                positionOffset - getPatternOffsetIndex(startIndex - 1));
    tmpValues[arrayOffset + endIndex] =
        (RLEPatternColumn)
            tmpValues[arrayOffset + endIndex].subColumnHead(
                endPosition - getPatternOffsetIndex(endIndex - 1));

    return new RLEColumn(
        arrayOffset + startIndex, endIndex - startIndex + 1, valueIsNull, null, tmpValues);
  }

  public Column getRegionRLE(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RLEColumn(
        arrayOffset + positionOffset, length, valueIsNull, patternOffsetIndex, values);
  }

  @Override
  public Column subColumn(int fromIndex) {
    // position correspoding to the index in raw data
    int index = 0;
    for (index = 0;
        index < this.positionCount && getPatternOffsetIndex(index) <= fromIndex;
        index++) ;

    if (index > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    int curOffset = getPatternOffsetIndex(index - 1);
    if (curOffset == fromIndex) {
      return new RLEColumn(
          arrayOffset + index, positionCount - index, valueIsNull, patternOffsetIndex, values);
    } else {
      /*reconstruct the values  */
      RLEPatternColumn[] tmpValues = new RLEPatternColumn[values.length];
      tmpValues = Arrays.copyOf(values, values.length);
      tmpValues[arrayOffset + index] =
          (RLEPatternColumn) tmpValues[arrayOffset + index].subColumn(fromIndex - curOffset);

      return new RLEColumn(
          arrayOffset + index, positionCount - index, valueIsNull, null, tmpValues);
    }
  }

  public Column subColumnRLE(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new RLEColumn(
        arrayOffset + fromIndex,
        positionCount - fromIndex,
        valueIsNull,
        patternOffsetIndex,
        values);
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
      return new RLEColumn(0, newCount, valueIsNull, patternOffsetIndex, values);
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
    return new RLEColumn(0, newCount, newValueIsNull, null, newValue);
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
    constructPatternOffsetIndex();
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
    values[arrayOffset + index].isNull()[position - accumulator] = false;
  }
}
