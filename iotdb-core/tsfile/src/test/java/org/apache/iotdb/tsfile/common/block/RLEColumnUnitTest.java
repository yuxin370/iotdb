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

package org.apache.iotdb.tsfile.common.block;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoderFactory;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoding;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

import static java.lang.Math.random;

public class RLEColumnUnitTest {

  private void testInternalRLE(RLEColumn input) {

    long expectedRetainedSize = input.getRetainedSizeInBytes();
    int positionCount = input.getPositionCount();
    ColumnEncoder encoder = ColumnEncoderFactory.get(ColumnEncoding.RLE_ARRAY);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      encoder.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Column output = encoder.readColumn(buffer, input.getDataType(), positionCount);

    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertFalse(output.mayHaveNull());
    Assert.assertEquals(expectedRetainedSize, output.getRetainedSizeInBytes());
    for (int i = 0; i < positionCount; i++) {
      Assert.assertEquals(input.getObject(i), output.getObject(i));
    }
  }

  private boolean[] generateArrayBoolean(int positionCount) {
    boolean[] bools = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      bools[i] = i % 2 == 0 ? true : false;
    }
    return bools;
  }

  private int[] generateArrayInt(int positionCount) {
    int[] ints = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      ints[i] = ((int) (random() * 100));
    }
    return ints;
  }

  private long[] generateArrayLong(int positionCount) {
    long[] longs = new long[positionCount];
    for (int i = 0; i < positionCount; i++) {
      longs[i] = (long) (random() * 100);
    }
    return longs;
  }

  private float[] generateArrayFloat(int positionCount) {
    float[] floats = new float[positionCount];
    for (int i = 0; i < positionCount; i++) {
      floats[i] = (float) (random() * 100);
    }
    return floats;
  }

  private double[] generateArrayDouble(int positionCount) {
    double[] doubles = new double[positionCount];
    for (int i = 0; i < positionCount; i++) {
      doubles[i] = ((double) random() * 100);
    }
    return doubles;
  }

  private Binary[] generateArrayBinary(int positionCount) {
    Binary[] binarys = new Binary[positionCount];
    for (int i = 0; i < positionCount; i++) {
      binarys[i] =
          new Binary(UUID.randomUUID().toString().substring(0, 5), TSFileConfig.STRING_CHARSET);
    }
    return binarys;
  }

  public void testGetRegion(RLEColumn input) {
    input = (RLEColumn) input.subColumn(15);
    RLEColumn getregion1 = (RLEColumn) input.getRegion(0, 20);
    for (int i = 0; i < 20; i++) {
      Assert.assertEquals(input.getObject(i), getregion1.getObject(i));
    }
    RLEColumn getregion2 = (RLEColumn) input.getRegion(30, 55);
    for (int i = 0, j = 30; i < 55; i++, j++) {
      Assert.assertEquals(input.getObject(j), getregion2.getObject(i));
    }
    RLEColumn getregion3 = (RLEColumn) input.getRegion(13, 34);
    for (int i = 0, j = 13; i < 34; i++, j++) {
      Assert.assertEquals(input.getObject(j), getregion3.getObject(i));
    }
    RLEColumn getregion4 = (RLEColumn) input.getRegion(53, 32);
    for (int i = 0, j = 53; i < 32; i++, j++) {
      Assert.assertEquals(input.getObject(j), getregion4.getObject(i));
    }
  }

  public void testSubColumn(RLEColumn input) {
    RLEColumn subColumn1 = (RLEColumn) input.subColumn(0);
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(input.getObject(i), subColumn1.getObject(i));
    }
    RLEColumn subColumn2 = (RLEColumn) input.subColumn(30);
    for (int i = 0, j = 30; j < 100; i++, j++) {
      Assert.assertEquals(input.getObject(j), subColumn2.getObject(i));
    }
    RLEColumn subColumn3 = (RLEColumn) input.subColumn(33);
    for (int i = 0, j = 33; j < 100; i++, j++) {
      Assert.assertEquals(input.getObject(j), subColumn3.getObject(i));
    }
    RLEColumn subColumn4 = (RLEColumn) input.subColumn(100);
    Assert.assertEquals(0, subColumn4.getPositionCount());
  }

  @Test
  public void testBooleanColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.BOOLEAN);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column = new BooleanColumn(1, Optional.empty(), new boolean[] {true});
      } else {
        column =
            new BooleanColumn(
                innnerPositionCount, Optional.empty(), generateArrayBoolean(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }

  @Test
  public void testIntColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.INT32);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column = new IntColumn(1, Optional.empty(), new int[] {0});
      } else {
        column =
            new IntColumn(
                innnerPositionCount, Optional.empty(), generateArrayInt(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }

  @Test
  public void testLongColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.INT64);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column = new LongColumn(1, Optional.empty(), new long[] {0});
      } else {
        column =
            new LongColumn(
                innnerPositionCount, Optional.empty(), generateArrayLong(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }

  @Test
  public void testFloatColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.FLOAT);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column = new FloatColumn(1, Optional.empty(), new float[] {0});
      } else {
        column =
            new FloatColumn(
                innnerPositionCount, Optional.empty(), generateArrayFloat(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }

  @Test
  public void testDoubleColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.DOUBLE);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column = new DoubleColumn(1, Optional.empty(), new double[] {0});
      } else {
        column =
            new DoubleColumn(
                innnerPositionCount, Optional.empty(), generateArrayDouble(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }

  @Test
  public void testTextColumn() {
    int patternCount = 10;
    int innnerPositionCount = 10;
    RLEColumnBuilder builder =
        new RLEColumnBuilder(null, innnerPositionCount * patternCount, TSDataType.TEXT);
    for (int i = 0; i < patternCount; i++) {
      Column column;
      if (i % 3 != 0) {
        column =
            new BinaryColumn(
                1, Optional.empty(), new Binary[] {new Binary("foo", TSFileConfig.STRING_CHARSET)});
      } else {
        column =
            new BinaryColumn(
                innnerPositionCount, Optional.empty(), generateArrayBinary(innnerPositionCount));
      }
      builder.writeColumn(column, innnerPositionCount);
    }
    RLEColumn input = (RLEColumn) builder.build();
    testInternalRLE(input);
    testSubColumn(input);
    testGetRegion(input);
  }
}
