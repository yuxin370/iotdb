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
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
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

public class RLEColumnEncoderTest {

  private void testInternalRLE(Column column) {
    final int positionCount = 100;

    Column input = new RLEPatternColumn(column, positionCount, 0);
    long expectedRetainedSize = input.getRetainedSizeInBytes();
    ColumnEncoder encoder = ColumnEncoderFactory.get(ColumnEncoding.RLE_PATTERN);

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
      Assert.assertEquals(column.getObject(0), output.getObject(i));
    }
  }

  private void testInternalBITPACKED(Column column, int positionCount) {
    Column input = new RLEPatternColumn(column, positionCount, 1);
    long expectedRetainedSize = input.getRetainedSizeInBytes();
    ColumnEncoder encoder = ColumnEncoderFactory.get(ColumnEncoding.RLE_PATTERN);

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
      Assert.assertEquals(column.getObject(i), output.getObject(i));
    }
  }

  @Test
  public void testBooleanColumn() {
    testInternalRLE(new BooleanColumn(1, Optional.empty(), new boolean[] {true}));
    int positionCount = 100;
    boolean[] bools = new boolean[positionCount];
    for (int i = 0; i < positionCount; i++) {
      bools[i] = random() > 0.5 ? true : false;
    }
    testInternalBITPACKED(new BooleanColumn(positionCount, Optional.empty(), bools), positionCount);
  }

  @Test
  public void testIntColumn() {
    testInternalRLE(new IntColumn(1, Optional.empty(), new int[] {0}));
    int positionCount = 100;
    int[] ints = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      ints[i] = (int) (random() * 100 + 1);
    }
    testInternalBITPACKED(new IntColumn(positionCount, Optional.empty(), ints), positionCount);
  }

  @Test
  public void testLongColumn() {
    testInternalRLE(new LongColumn(1, Optional.empty(), new long[] {0L}));
    int positionCount = 100;
    long[] longs = new long[positionCount];
    for (int i = 0; i < positionCount; i++) {
      longs[i] = (long) (random() * 100 + 1);
    }
    testInternalBITPACKED(new LongColumn(positionCount, Optional.empty(), longs), positionCount);
  }

  @Test
  public void testFloatColumn() {
    testInternalRLE(new FloatColumn(1, Optional.empty(), new float[] {0.0F}));
    int positionCount = 100;
    float[] floats = new float[positionCount];
    for (int i = 0; i < positionCount; i++) {
      floats[i] = (float) (random() * 100 + 1);
    }
    testInternalBITPACKED(new FloatColumn(positionCount, Optional.empty(), floats), positionCount);
  }

  @Test
  public void testDoubleColumn() {
    testInternalRLE(new DoubleColumn(1, Optional.empty(), new double[] {0.0D}));
    int positionCount = 100;
    double[] doubles = new double[positionCount];
    for (int i = 0; i < positionCount; i++) {
      doubles[i] = (double) (random() * 100 + 1);
    }
    testInternalBITPACKED(
        new DoubleColumn(positionCount, Optional.empty(), doubles), positionCount);
  }

  @Test
  public void testTextColumn() {
    testInternalRLE(
        new BinaryColumn(
            1, Optional.empty(), new Binary[] {new Binary("foo", TSFileConfig.STRING_CHARSET)}));

    int positionCount = 100;
    Binary[] texts = new Binary[positionCount];
    for (int i = 0; i < positionCount; i++) {
      texts[i] =
          new Binary(UUID.randomUUID().toString().substring(0, 5), TSFileConfig.STRING_CHARSET);
    }
    testInternalBITPACKED(new BinaryColumn(positionCount, Optional.empty(), texts), positionCount);
  }
}
