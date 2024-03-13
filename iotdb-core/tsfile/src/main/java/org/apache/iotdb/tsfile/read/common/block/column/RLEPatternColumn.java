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
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.apache.commons.lang3.NotImplementedException;
import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class RLEPatternColumn implements Column {
  private static final Logger LOGGER = LoggerFactory.getLogger(RLEPatternColumn.class);

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(RLEPatternColumn.class).instanceSize();

  private final Column value;
  private final int positionCount;
  private final RunLengthMode type;

  public RLEPatternColumn(Column value, int positionCount, int type) {
    requireNonNull(value, "value is null");

    if (value instanceof RLEPatternColumn) {
      this.value = ((RLEPatternColumn) value).getValue();
    } else {
      this.value = value;
    }

    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }

    this.positionCount = positionCount;
    this.type = type == 0 ? RunLengthMode.RLE : RunLengthMode.BIT_PACKED;
    checkRLEValid();
  }

  public RLEPatternColumn(Column value, int positionCount, RunLengthMode type) {
    requireNonNull(value, "value is null");

    if (value instanceof RLEPatternColumn) {
      this.value = ((RLEPatternColumn) value).getValue();
    } else {
      this.value = value;
    }

    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }

    this.positionCount = positionCount;
    this.type = type;
    checkRLEValid();
  }

  private void checkRLEValid() {
    if (this.type == RunLengthMode.RLE && value.getPositionCount() != 1) {
      throw new IllegalArgumentException(
          format(
              "Expected value to contain a single position but has %s positions",
              value.getPositionCount()));
    }
  }

  public Column getValue() {
    return value;
  }

  public RunLengthMode getMode() {
    return type;
  }

  public boolean isRLEMode() {
    return type == RunLengthMode.RLE ? true : false;
  }

  @Override
  public TSDataType getDataType() {
    return value.getDataType();
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.RLE_PATTERN;
  }

  @Override
  public boolean getBoolean(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getBoolean(0);
    } else {
      return value.getBoolean(position);
    }
  }

  @Override
  public int getInt(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getInt(0);
    } else {
      return value.getInt(position);
    }
  }

  @Override
  public long getLong(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getLong(0);
    } else {
      return value.getLong(position);
    }
  }

  @Override
  public float getFloat(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getFloat(0);
    } else {
      return value.getFloat(position);
    }
  }

  @Override
  public double getDouble(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getDouble(0);
    } else {
      return value.getDouble(position);
    }
  }

  @Override
  public Binary getBinary(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getBinary(0);
    } else {
      return value.getBinary(position);
    }
  }

  @Override
  public Object getObject(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position " + position + " is not less than positionCount " + this.getPositionCount());
    }
    if (type == RunLengthMode.RLE) {
      return value.getObject(0);
    } else {
      return value.getObject(position);
    }
  }

  @Override
  public boolean[] getBooleans() {
    if (this.type == RunLengthMode.RLE) {
      boolean[] res = new boolean[positionCount];
      Arrays.fill(res, value.getBoolean(0));
      return res;
    } else {
      return value.getBooleans();
    }
  }

  @Override
  public int[] getInts() {
    if (this.type == RunLengthMode.RLE) {
      int[] res = new int[positionCount];
      Arrays.fill(res, value.getInt(0));
      return res;
    } else {
      return value.getInts();
    }
  }

  @Override
  public long[] getLongs() {
    if (this.type == RunLengthMode.RLE) {
      long[] res = new long[positionCount];
      Arrays.fill(res, value.getLong(0));
      return res;
    } else {
      return value.getLongs();
    }
  }

  @Override
  public float[] getFloats() {
    if (this.type == RunLengthMode.RLE) {
      float[] res = new float[positionCount];
      Arrays.fill(res, value.getFloat(0));
      return res;
    } else {
      return value.getFloats();
    }
  }

  @Override
  public double[] getDoubles() {
    if (this.type == RunLengthMode.RLE) {
      double[] res = new double[positionCount];
      Arrays.fill(res, value.getDouble(0));
      return res;
    } else {
      return value.getDoubles();
    }
  }

  @Override
  public Binary[] getBinaries() {
    if (this.type == RunLengthMode.RLE) {
      Binary[] res = new Binary[positionCount];
      Arrays.fill(res, value.getBinary(0));
      return res;
    } else {
      return value.getBinaries();
    }
  }

  @Override
  public Object[] getObjects() {
    if (this.type == RunLengthMode.RLE) {
      Object[] res = new Object[positionCount];
      Arrays.fill(res, value.getObject(0));
      return res;
    } else {
      return value.getObjects();
    }
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    if (this.type == RunLengthMode.RLE) {
      return value.getTsPrimitiveType(0);
    } else {
      return value.getTsPrimitiveType(position);
    }
  }

  @Override
  public boolean mayHaveNull() {
    return value.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    if (position >= positionCount) {
      throw new IllegalArgumentException(
          "position "
              + position
              + " is not less than positionCount "
              + this.getPositionCount()
              + " and value.length = "
              + value.getPositionCount());
    }
    return value.isNull(position);
  }

  @Override
  public boolean[] isNull() {
    if (this.type == RunLengthMode.RLE) {
      boolean[] res = new boolean[positionCount];
      Arrays.fill(res, value.isNull(0));
      return res;
    } else {
      return value.isNull();
    }
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + value.getRetainedSizeInBytes();
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RLEPatternColumn(value, length, type);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    if (type == RunLengthMode.RLE) {
      return new RLEPatternColumn(value, positionCount - fromIndex, type);
    } else {
      Column tmpValue = value.subColumn(fromIndex);
      return new RLEPatternColumn(tmpValue, positionCount - fromIndex, type);
    }
  }

  public Column subColumnHead(int toIndex) {
    if (toIndex > positionCount) {
      throw new IllegalArgumentException("toIndex is not valid");
    }
    return new RLEPatternColumn(value, toIndex, type);
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
    if (type == RunLengthMode.RLE || newCount == positionCount) {
      return new RLEPatternColumn(value, newCount, type);
    } else {
      return new RLEPatternColumn(value.subColumn(valueRetained), newCount, type);
    }
  }

  @Override
  public void reverse() {
    value.reverse();
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  public int getSizeInBytesPerPosition() {
    if (value instanceof IntColumn) {
      return IntColumn.SIZE_IN_BYTES_PER_POSITION;
    } else if (value instanceof LongColumn) {
      return LongColumn.SIZE_IN_BYTES_PER_POSITION;
    } else if (value instanceof DoubleColumn) {
      return DoubleColumn.SIZE_IN_BYTES_PER_POSITION;
    } else if (value instanceof FloatColumn) {
      return FloatColumn.SIZE_IN_BYTES_PER_POSITION;
    } else if (value instanceof BooleanColumn) {
      return BooleanColumn.SIZE_IN_BYTES_PER_POSITION;
    } else {
      throw new NotImplementedException(
          value.toString() + " can not get SIZE_IN_BYTES_PER_POSITION");
    }
  }

  protected enum RunLengthMode {
    RLE((byte) 0),
    BIT_PACKED((byte) 1);

    private final byte value;

    private RunLengthMode(byte value) {
      this.value = value;
    }

    public static RunLengthMode deserializeFrom(ByteBuffer buffer) {
      return getColumnEncoding(buffer.get());
    }

    public void serializeTo(DataOutputStream stream) throws IOException {
      stream.writeByte(value);
    }

    private static RunLengthMode getColumnEncoding(byte value) {
      switch (value) {
        case 0:
          return RLE;
        case 1:
          return BIT_PACKED;
        default:
          throw new IllegalArgumentException("Invalid value: " + value);
      }
    }
  }
}
