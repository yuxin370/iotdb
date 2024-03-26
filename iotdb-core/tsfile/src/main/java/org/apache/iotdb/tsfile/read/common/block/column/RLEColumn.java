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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.RLEPattern;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class RLEColumn implements Column {
  private static final Logger LOGGER = LoggerFactory.getLogger(RLEColumn.class);

  private static final int INSTANCE_SIZE = ClassLayout.parseClass(RLEColumn.class).instanceSize();

  private final int arrayOffset; // offset of values Array
  private final int positionCount;
  private int patternCount; // count of valid RlePatterns
  private final RLEPattern[] values;
  private final int[]
      patternOffsetIndex; // patternOffsetIndex[i] refers to the offset of values[i].getObject(0) in
  // all data
  private int
      curIndex; // Marking the latest read column index, which can effectively save traversal time
  // when data is continuously read.

  public RLEColumn(
      int positionCount, int patternCount, RLEPattern[] values, int[] patternOffsetIndex) {
    this(0, positionCount, patternCount, values, patternOffsetIndex, 0);
  }

  public RLEColumn(
      int arrayOffset,
      int positionCount,
      int patternCount,
      RLEPattern[] values,
      int[] patternOffsetIndex) {
    this(arrayOffset, positionCount, patternCount, values, patternOffsetIndex, 0);
  }

  RLEColumn(
      int arrayOffset,
      int positionCount,
      int patternCount,
      RLEPattern[] values,
      int[] patternOffsetIndex,
      int curIndex) {
    requireNonNull(values, "values is null");

    if (arrayOffset < 0) {
      throw new IllegalArgumentException("arrayOffset is negative");
    }
    this.arrayOffset = arrayOffset;
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;

    if (patternCount < 0) {
      throw new IllegalArgumentException("patternCount is negative");
    }
    this.patternCount = patternCount;

    if (values.length - arrayOffset < patternCount) {
      throw new IllegalArgumentException("values length is less than patternCount");
    }

    this.values = values;

    if (patternOffsetIndex != null && patternOffsetIndex.length - arrayOffset < patternCount) {
      throw new IllegalArgumentException("patternOffsetIndex length is less than positionCount");
    }
    this.patternOffsetIndex = patternOffsetIndex;

    this.curIndex = curIndex;
  }

  private int getCurIndex(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          " position: " + position + " out of the bound of positionCount: " + positionCount);
    }
    int index;
    if (position >= getPatternOffsetIndex(curIndex)) {
      /** check if curIndex hit */
      if (position < getPatternOffsetIndex(curIndex + 1)) {
        /** hit */
        return curIndex;
      } else {
        /** miss, traverse from curIndex + 1 and update curIndex */
        for (index = curIndex + 1;
            index < this.patternCount && position >= getPatternOffsetIndex(index);
            index++) ;
        curIndex = index - 1;
        return curIndex;
      }
    }

    /** miss, traverse from scratch and reset curIndex */
    for (index = 0; index < this.patternCount && position >= getPatternOffsetIndex(index); index++)
      ;
    curIndex = index - 1;
    return curIndex;
  }

  @Override
  public TSDataType getDataType() {
    return (values[0]).getValue().getDataType();
  }

  @Override
  public boolean getBoolean(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getBoolean(0)
        : value.getBoolean(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public int getInt(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getInt(0)
        : value.getInt(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public long getLong(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getLong(0)
        : value.getLong(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public float getFloat(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getFloat(0)
        : value.getFloat(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public double getDouble(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getDouble(0)
        : value.getDouble(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public Binary getBinary(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getBinary(0)
        : value.getBinary(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public Column getColumn(int index) {
    if (index < 0 || index >= patternCount) {
      throw new IllegalArgumentException(
          "getColumn index: " + index + " is illegal. where patternCount = " + patternCount);
    }
    return values[arrayOffset + index].getValue();
  }

  @Override
  public Object getObject(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getObject(0)
        : value.getObject(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public boolean[] getBooleans() {
    boolean[] res = new boolean[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(
            res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getBoolean(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getBoolean(j);
        }
      }
    }
    return res;
  }

  @Override
  public int[] getInts() {
    int[] res = new int[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getInt(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getInt(j);
        }
      }
    }
    return res;
  }

  @Override
  public long[] getLongs() {
    long[] res = new long[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getLong(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getLong(j);
        }
      }
    }
    return res;
  }

  @Override
  public float[] getFloats() {
    float[] res = new float[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getFloat(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getFloat(j);
        }
      }
    }
    return res;
  }

  @Override
  public double[] getDoubles() {
    double[] res = new double[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(
            res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getDouble(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getDouble(j);
        }
      }
    }
    return res;
  }

  @Override
  public Binary[] getBinaries() {
    Binary[] res = new Binary[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(
            res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getBinary(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getBinary(j);
        }
      }
    }
    return res;
  }

  @Override
  public Object[] getObjects() {
    Object[] res = new Object[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(
            res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.getObject(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.getObject(j);
        }
      }
    }
    return res;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.RLE_ARRAY;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.getTsPrimitiveType(0)
        : value.getTsPrimitiveType(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public boolean mayHaveNull() {
    for (int i = 0; i < patternCount; i++) {
      if (values[arrayOffset + i].getValue().mayHaveNull()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isNull(int position) {
    int curIndex = getCurIndex(position);
    Column value = values[arrayOffset + curIndex].getValue();
    return value.getPositionCount() == 1
        ? value.isNull(0)
        : value.isNull(position - getPatternOffsetIndex(curIndex));
  }

  @Override
  public boolean[] isNull() {
    boolean[] res = new boolean[positionCount];
    for (int i = 0; i < patternCount; i++) {
      Column value = values[arrayOffset + i].getValue();
      int curPatternActualPositionCount = value.getPositionCount();
      if (curPatternActualPositionCount == 1) {
        Arrays.fill(res, getPatternOffsetIndex(i), getPatternOffsetIndex(i + 1), value.isNull(0));
      } else {
        int startIndex = getPatternOffsetIndex(i);
        for (int j = 0; j < curPatternActualPositionCount; j++) {
          res[startIndex + j] = value.isNull(j);
        }
      }
    }
    return res;
  }

  /** get positionCount, which is the number of the RLEPatternColumn */
  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    long valuesRetainedSizeInBytes = 0;
    for (int i = 0; i < patternCount; i++) {
      valuesRetainedSizeInBytes += values[arrayOffset + i].getValue().getRetainedSizeInBytes();
    }
    return INSTANCE_SIZE + sizeOfIntArray(patternCount + 1) + valuesRetainedSizeInBytes;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);

    int endPositionOffset = positionOffset + length - 1;
    int startIndex = getCurIndex(positionOffset);
    int endIndex = getCurIndex(endPositionOffset);

    /*reconstruct the values  */
    RLEPattern[] valuesTmp = new RLEPattern[values.length];
    for (int i = 0; i < patternCount; i++) {
      valuesTmp[arrayOffset + i] = values[arrayOffset + i].deepCopy();
    }
    int subFromOffset = positionOffset - getPatternOffsetIndex(startIndex);
    int subToOffset = endPositionOffset - getPatternOffsetIndex(endIndex);
    int[] patternOffsetIndexTmp = Arrays.copyOf(patternOffsetIndex, patternOffsetIndex.length);

    if (startIndex == endIndex) {
      valuesTmp[arrayOffset + endIndex].getRegion(subFromOffset, length);

      patternOffsetIndexTmp[arrayOffset + endIndex] = 0;
      patternOffsetIndexTmp[arrayOffset + endIndex + 1] = length;
    } else {
      valuesTmp[arrayOffset + startIndex].subColumn(subFromOffset);
      valuesTmp[arrayOffset + endIndex].getRegion(0, subToOffset + 1);

      patternOffsetIndexTmp[arrayOffset + startIndex] = 0;
      for (int i = arrayOffset + startIndex + 1; i <= arrayOffset + endIndex; i++) {
        patternOffsetIndexTmp[i] = patternOffsetIndexTmp[i] - positionOffset;
      }
      patternOffsetIndexTmp[arrayOffset + endIndex + 1] = length;
    }
    return new RLEColumn(
        arrayOffset + startIndex,
        length,
        endIndex - startIndex + 1,
        valuesTmp,
        patternOffsetIndexTmp);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex == positionCount) {
      return new RLEColumn(arrayOffset + patternCount, 0, 0, values, patternOffsetIndex);
    }

    int curIndex = getCurIndex(fromIndex);
    int curOffset = getPatternOffsetIndex(curIndex);
    int[] patternOffsetIndexTmp = Arrays.copyOf(patternOffsetIndex, patternOffsetIndex.length);

    if (curOffset == fromIndex) {
      for (int i = arrayOffset + curIndex; i <= arrayOffset + patternCount; i++) {
        patternOffsetIndexTmp[i] = patternOffsetIndexTmp[i] - curOffset;
      }
      return new RLEColumn(
          arrayOffset + curIndex,
          positionCount - fromIndex,
          patternCount - curIndex,
          values,
          patternOffsetIndexTmp);
    } else {
      /*reconstruct the values  */
      int subFromIndex = fromIndex - curOffset;
      RLEPattern[] valuesTmp = new RLEPattern[values.length];
      for (int i = 0; i < patternCount; i++) {
        valuesTmp[arrayOffset + i] = values[arrayOffset + i].deepCopy();
      }
      valuesTmp[arrayOffset + curIndex].subColumn(subFromIndex);

      patternOffsetIndexTmp[arrayOffset + curIndex] = 0;
      for (int i = arrayOffset + curIndex + 1; i <= arrayOffset + patternCount; i++) {
        patternOffsetIndexTmp[i] = patternOffsetIndexTmp[i] - fromIndex;
      }

      return new RLEColumn(
          arrayOffset + curIndex,
          positionCount - fromIndex,
          patternCount - curIndex,
          valuesTmp,
          patternOffsetIndexTmp);
    }
  }

  @Override
  public void reverse() {
    for (int i = arrayOffset, j = arrayOffset + patternCount - 1; i < j; i++, j--) {
      RLEPattern valueTmp = values[i];
      values[i] = values[j];
      values[j] = valueTmp;
      values[i].reverseValue();
      values[j].reverseValue();
    }

    // reverse patternOffsetIndex
    patternOffsetIndex[arrayOffset] = 0;
    for (int i = arrayOffset + 1, j = arrayOffset + patternCount; j > arrayOffset; i++, j--) {
      patternOffsetIndex[i] = patternOffsetIndex[i - 1] + values[i - 1].getLogicPositionCount();
    }
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  public int getPatternOffsetIndex(int index) {
    return patternOffsetIndex[arrayOffset + index];
  }

  public int getLogicPositionCount(int index) {
    if (index < 0 || index >= patternCount) {
      throw new IllegalArgumentException(
          "getLogicPositionCount index: "
              + index
              + " is illegal. where patternCount = "
              + patternCount);
    }
    return values[arrayOffset + index].getLogicPositionCount();
  }

  public int getPatternCount() {
    return patternCount;
  }

  public void print() {
    LOGGER.info(
        "-----------------------------[tyx print tsBlock start]-------------------------------");
    LOGGER.info(
        "arrayOffset = "
            + arrayOffset
            + "  postionCount = "
            + positionCount
            + "  patternCount = "
            + patternCount);
    for (int i = 0; i < patternCount; i++) {
      if (values[arrayOffset + i].getValue().getPositionCount() == 1) {
        LOGGER.info(
            "pattern "
                + i
                + ": physical length= "
                + values[arrayOffset + i].getValue().getPositionCount()
                + "  logical Count= "
                + values[arrayOffset + i].getLogicPositionCount()
                + " offsetindex["
                + i
                + "]= "
                + getPatternOffsetIndex(i)
                + " value = "
                + values[arrayOffset + i].getValue().getObject(0));
      } else {
        LOGGER.info(
            "pattern "
                + i
                + ": physical length= "
                + values[arrayOffset + i].getValue().getPositionCount()
                + "  logical Count= "
                + values[arrayOffset + i].getLogicPositionCount()
                + " offsetindex["
                + i
                + "]= "
                + getPatternOffsetIndex(i));
      }
    }
    LOGGER.info("offsetindex[" + patternCount + "]= " + getPatternOffsetIndex(patternCount));
    LOGGER.info(
        "------------------------------[tyx print tsBlock end]--------------------------------");
  }
}
