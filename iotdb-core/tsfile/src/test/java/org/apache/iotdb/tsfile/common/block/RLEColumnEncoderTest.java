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
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static java.lang.Math.random;

public class RLEColumnEncoderTest {

  private void testInternalRLE(
      int positionCount, boolean[] nullIndicators, RLEPatternColumn[] columns) {

    Column input = new RLEColumn(positionCount, Optional.of(nullIndicators), columns);
    long expectedRetainedSize = input.getRetainedSizeInBytes();
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
    Column output = encoder.readColumn(buffer, columns[1].getDataType(), positionCount);

    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertTrue(output.mayHaveNull());
    Assert.assertEquals(expectedRetainedSize, output.getRetainedSizeInBytes());
    for (int i = 0; i < positionCount; i++) {
      RLEPatternColumn expected = ((RLEColumn) input).getRLEPattern(i);
      RLEPatternColumn getted = ((RLEColumn) output).getRLEPattern(i);
      if (expected == null && getted == null) {
        continue;
      }
      int count = expected.getPositionCount();
      for (int j = 0; j < count; j++) {
        Assert.assertEquals(expected.getObject(j), getted.getObject(j));
      }
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
      ints[i] = ((int) random() * 100);
    }
    return ints;
  }

  private long[] generateArrayLong(int positionCount) {
    long[] longs = new long[positionCount];
    for (int i = 0; i < positionCount; i++) {
      longs[i] = ((long) random() * 100);
    }
    return longs;
  }

  private float[] generateArrayFloat(int positionCount) {
    float[] floats = new float[positionCount];
    for (int i = 0; i < positionCount; i++) {
      floats[i] = ((float) random() * 100);
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

  private int[] generateConstantArrayInt(int positionCount) {
    int[] ints = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      ints[i] = (i);
    }
    return ints;
  }

  @Test
  public void testGetRegion() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    Arrays.fill(nullIndicators, false);
    for (int i = 0; i < positionCount; i++) {
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(1, Optional.empty(), new int[] {1}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(
                    positionCount, Optional.empty(), generateConstantArrayInt(positionCount)),
                positionCount,
                1);
      }
    }
    RLEColumn input = new RLEColumn(positionCount, Optional.of(nullIndicators), columns);
    RLEColumn getregion1 = (RLEColumn) input.getRegion(0, 20);
    for (int i = 0; i < 20; i++) {
      Assert.assertEquals((int) input.getObject(i), (int) getregion1.getObject(i));
    }
    RLEColumn getregion2 = (RLEColumn) input.getRegion(30, 70);
    for (int i = 0, j = 30; i < 70; i++, j++) {
      Assert.assertEquals((int) input.getObject(j), (int) getregion2.getObject(i));
    }
    RLEColumn getregion3 = (RLEColumn) input.getRegion(13, 34);
    for (int i = 0, j = 13; i < 34; i++, j++) {
      Assert.assertEquals((int) input.getObject(j), (int) getregion3.getObject(i));
    }
  }

  @Test
  public void testSubColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    Arrays.fill(nullIndicators, false);
    for (int i = 0; i < positionCount; i++) {
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(1, Optional.empty(), new int[] {1}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(
                    positionCount, Optional.empty(), generateConstantArrayInt(positionCount)),
                positionCount,
                1);
      }
    }
    RLEColumn input = new RLEColumn(positionCount, Optional.of(nullIndicators), columns);
    RLEColumn subColumn1 = (RLEColumn) input.subColumn(0);
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals((int) input.getObject(i), (int) subColumn1.getObject(i));
    }
    RLEColumn subColumn2 = (RLEColumn) input.subColumn(30);
    for (int i = 0, j = 30; j < 100; i++, j++) {
      Assert.assertEquals((int) input.getObject(j), (int) subColumn2.getObject(i));
    }
    RLEColumn subColumn3 = (RLEColumn) input.subColumn(33);
    for (int i = 0, j = 13; j < 100; i++, j++) {
      Assert.assertEquals((int) input.getObject(j), (int) subColumn3.getObject(i));
    }
    RLEColumn subColumn4 = (RLEColumn) input.subColumn(100);
    for (int i = 0, j = 100; j < 100; i++, j++) {
      Assert.assertEquals((int) input.getObject(j), (int) subColumn4.getObject(i));
    }
  }

  @Test
  public void testBooleanColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new BooleanColumn(1, Optional.empty(), new boolean[] {true}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new BooleanColumn(
                    positionCount, Optional.empty(), generateArrayBoolean(positionCount)),
                positionCount,
                1);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }

  @Test
  public void testIntColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(1, Optional.empty(), new int[] {0}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new IntColumn(positionCount, Optional.empty(), generateArrayInt(positionCount)),
                positionCount,
                1);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }

  @Test
  public void testLongColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new LongColumn(1, Optional.empty(), new long[] {0}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new LongColumn(positionCount, Optional.empty(), generateArrayLong(positionCount)),
                positionCount,
                1);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }

  @Test
  public void testFloatColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new FloatColumn(1, Optional.empty(), new float[] {0}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new FloatColumn(positionCount, Optional.empty(), generateArrayFloat(positionCount)),
                positionCount,
                1);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }

  @Test
  public void testDoubleColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new DoubleColumn(1, Optional.empty(), new double[] {0}), positionCount, 0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new DoubleColumn(
                    positionCount, Optional.empty(), generateArrayDouble(positionCount)),
                positionCount,
                1);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }

  @Test
  public void testTextColumn() {
    int positionCount = 10;
    RLEPatternColumn[] columns = new RLEPatternColumn[positionCount];
    boolean[] nullIndicators = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 5 == 0;
      if (nullIndicators[i]) {
        continue;
      }
      if (i % 3 != 0) {
        columns[i] =
            new RLEPatternColumn(
                new BinaryColumn(
                    1,
                    Optional.empty(),
                    new Binary[] {new Binary("foo", TSFileConfig.STRING_CHARSET)}),
                positionCount,
                0);
      } else {
        columns[i] =
            new RLEPatternColumn(
                new BinaryColumn(
                    positionCount, Optional.empty(), generateArrayBinary(positionCount)),
                positionCount,
                0);
      }
    }
    testInternalRLE(positionCount, nullIndicators, columns);
  }
}
