/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for transformeral information
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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

public class RLEArithmeticBinaryColumnTransformerTest {

  private static final Type returnType = TypeFactory.getType(TSDataType.DOUBLE);

  private static final Type booleanType = TypeFactory.getType(TSDataType.BOOLEAN);

  private static final int POSITION_COUNT = 1000;

  private final double[] addResult = new double[1000];
  private final double[] multiResult = new double[1000];
  private final double[] diviResult = new double[1000];
  private final double[] modResult = new double[1000];
  private final double[] subResult = new double[1000];

  private static IdentityColumnTransformer leftOperand;

  private static IdentityColumnTransformer rightOperand;

  private double[] generateArrayDouble(int positionCount, double index) {
    double[] doubles = new double[positionCount];
    for (int i = 0; i < positionCount; i++) {
      doubles[i] = ((double) index + i);
    }
    return doubles;
  }

  @Before
  public void setUp() {
    Arrays.fill(diviResult, 1);
    Arrays.fill(modResult, 0);
    Arrays.fill(subResult, 0);

    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.RLEPATTERN, TSDataType.RLEPATTERN));
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder leftColumnBuilder = tsBlockBuilder.getColumnBuilder(0);
    ColumnBuilder rightColumnBuilder = tsBlockBuilder.getColumnBuilder(1);

    int index = 1;
    for (int j = 0; j < 100; j++) {
      int positionCount = 10;
      RLEPatternColumn column;
      if (j % 3 != 0) {
        column =
            new RLEPatternColumn(
                new DoubleColumn(1, Optional.empty(), new double[] {index}), positionCount, 0);
      } else {
        column =
            new RLEPatternColumn(
                new DoubleColumn(
                    positionCount, Optional.empty(), generateArrayDouble(positionCount, index)),
                positionCount,
                1);
      }

      int curIndex = index;
      for (int i = 0; i < positionCount; i++, index++) {
        timeColumnBuilder.writeLong(index);
        if (j % 3 != 0) {
          addResult[index - 1] = curIndex * 2.0;
          multiResult[index - 1] = curIndex * curIndex;
        } else {
          addResult[index - 1] = index * 2.0;
          multiResult[index - 1] = index * index;
        }
      }

      (leftColumnBuilder).writeObject(column);
      (rightColumnBuilder).writeObject(column);
      tsBlockBuilder.declarePositions(positionCount);
    }
    TsBlock tsBlock = tsBlockBuilder.build();

    leftOperand = new IdentityColumnTransformer(returnType, 0);
    rightOperand = new IdentityColumnTransformer(returnType, 1);
    leftOperand.addReferenceCount();
    rightOperand.addReferenceCount();
    leftOperand.initFromTsBlock(tsBlock);
    rightOperand.initFromTsBlock(tsBlock);
  }

  @Test
  public void testAddition() {
    BinaryColumnTransformer transformer =
        new ArithmeticAdditionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(addResult[i], res.getDouble(i), 0.001);
    }
  }

  @Test
  public void testSubtraction() {
    BinaryColumnTransformer transformer =
        new ArithmeticSubtractionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(subResult[i], res.getDouble(i), 0.001);
    }
  }

  @Test
  public void testMultiplication() {
    BinaryColumnTransformer transformer =
        new ArithmeticMultiplicationColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(multiResult[i], res.getDouble(i), 0.001);
    }
  }

  @Test
  public void testDivision() {
    BinaryColumnTransformer transformer =
        new ArithmeticDivisionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(diviResult[i], res.getDouble(i), 0.001);
    }
  }

  @Test
  public void testModulo() {
    BinaryColumnTransformer transformer =
        new ArithmeticModuloColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(modResult[i], res.getDouble(i), 0.001);
    }
  }
}
