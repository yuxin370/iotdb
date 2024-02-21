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

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class RLEColumn implements Column {

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

  @Override
  public Object getObject(int position) {
    return getRLEPattern(position);
  }

  @Override
  public TSDataType getDataType() {
    // if (valueIsNull == null) {
    //   return values[arrayOffset].getDataType();
    // } else {
    //   int i;
    //   for (i = arrayOffset; i < arrayOffset + positionCount && valueIsNull[i] == true; i++) ;
    //   if (i < arrayOffset + positionCount) {
    //     return values[arrayOffset + i].getDataType();
    //   } else {
    //     return TSDataType.UNKNOWN;
    //   }
    // }
    return TSDataType.RLEPATTERN;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.RLE_ARRAY;
  }

  @Override
  public Object[] getObjects() {
    return values;
  }

  // @Override
  // public TsPrimitiveType getTsPrimitiveType(int position) {
  //   if(valueIsNull != null && !valueIsNull[arrayOffset]){
  //     return values[arrayOffset + position].getTsPrimitiveType(0);
  //   }else{
  //     return null;
  //   }
  // }

  @Override
  public boolean mayHaveNull() {
    return valueIsNull != null;
  }

  @Override
  public boolean isNull(int position) {
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

  @Override
  public int getPositionCount() {
    return positionCount;
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
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new RLEColumn(arrayOffset + fromIndex, positionCount - fromIndex, valueIsNull, values);
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
}
