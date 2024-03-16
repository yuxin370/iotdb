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
  public static final RLEColumn NULL_VALUE_BLOCK =
      new RLEColumn(0, 1, new boolean[] {true}, null, new RLEPatternColumn[1]);

  private final ColumnBuilderStatus columnBuilderStatus;
  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private boolean hasNullValue;
  private boolean hasNonNullValue;

  // it is assumed that these arrays are the same length
  private boolean[] valueIsNull = new boolean[0];
  private RLEPatternColumn[] values = new RLEPatternColumn[0];

  private long retainedSizeInBytes;

  public RLEColumnBuilder(ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    this.columnBuilderStatus = columnBuilderStatus;
    this.initialEntryCount = max(expectedEntries, 1);

    updateDataSize();
  }

  public ColumnBuilder writeRLEPattern(RLEPatternColumn value) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = value;

    hasNonNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(value.getInstanceSize());
    }
    return this;
  }

  /** Write RLEPattern and only preserve the retained values */
  public ColumnBuilder writeRLEPattern(RLEPatternColumn value, boolean[] valueRetained) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = (RLEPatternColumn) value.subColumn(valueRetained);

    hasNonNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(value.getInstanceSize());
    }
    return this;
  }

  /** Write an Object to the current entry, which should be the RLEPattern type; */
  @Override
  public ColumnBuilder writeObject(Object value) {
    if (value instanceof RLEPatternColumn) {
      writeRLEPattern((RLEPatternColumn) value);
      return this;
    }
    throw new UnSupportedDataTypeException("RLEColumn only support RLEPattern data type");
  }

  @Override
  public ColumnBuilder write(Column column, int index) {
    // if here need to be modified to the raw index ?
    // current RLEPattern index supported.
    if (!(column instanceof RLEColumn)) {
      throw new UnsupportedOperationException(
          "for RLEColumnBuilder.write, only RLEColumn supported.");
    } else {
      return writeRLEPattern(((RLEColumn) column).getRLEPattern(index));
    }
  }

  @Override
  public ColumnBuilder writeRLEPattern(Column column, int index) {
    if (!(column instanceof RLEColumn)) {
      throw new UnsupportedOperationException(
          "for RLEColumnBuilder.write, only RLEColumn supported.");
    } else {
      return writeRLEPattern(((RLEColumn) column).getRLEPattern(index));
    }
  }

  @Override
  public ColumnBuilder appendNull() {
    if (values.length <= positionCount) {
      growCapacity();
    }

    valueIsNull[positionCount] = true;

    hasNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(IntColumn.SIZE_IN_BYTES_PER_POSITION);
    }
    return this;
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      return new RunLengthEncodedColumn(NULL_VALUE_BLOCK, positionCount);
    }
    return new RLEColumn(0, positionCount, hasNullValue ? valueIsNull : null, null, values);
  }

  @Override
  public TSDataType getDataType() {
    // int length = values.length;
    // while (length > 0) {
    //   if (valueIsNull[length - 1] == false) {
    //     return values[length - 1].getDataType();
    //   }
    //   length--;
    // }
    // return TSDataType.UNKNOWN;
    return TSDataType.RLEPATTERN;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    return new RLEColumnBuilder(columnBuilderStatus, calculateBlockResetSize(positionCount));
  }

  private void growCapacity() {
    int newSize;
    if (initialized) {
      newSize = ColumnUtil.calculateNewArraySize(values.length);
    } else {
      newSize = initialEntryCount;
      initialized = true;
    }

    valueIsNull = Arrays.copyOf(valueIsNull, newSize);
    values = Arrays.copyOf(values, newSize);
    updateDataSize();
  }

  private void updateDataSize() {
    retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    if (columnBuilderStatus != null) {
      retainedSizeInBytes += ColumnBuilderStatus.INSTANCE_SIZE;
    }
  }
}
