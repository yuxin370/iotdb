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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ArithmeticBinaryColumnTransformer extends BinaryColumnTransformer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ArithmeticBinaryColumnTransformer.class);

  protected ArithmeticBinaryColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  private void doTransformBetweenRLE(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    int leftPatternCount = ((RLEColumn) leftColumn).getPatternCount();
    int rightPatternCount = ((RLEColumn) rightColumn).getPatternCount();
    int leftIndex = 0, rightIndex = 0;
    int curLeft = 0, curRight = 0;
    Column leftPatternColumn = ((RLEColumn) leftColumn).getColumn(leftIndex);
    Column rightPatternColumn = ((RLEColumn) rightColumn).getColumn(rightIndex);
    boolean isRLELeft = leftPatternColumn.getPositionCount() == 1;
    boolean isRLERight = rightPatternColumn.getPositionCount() == 1;
    int curLeftPositionCount = ((RLEColumn) leftColumn).getLogicPositionCount(leftIndex);
    int curRightPositionCount = ((RLEColumn) rightColumn).getLogicPositionCount(rightIndex);
    int index = 0;
    int length = 0;
    while (index < positionCount) {
      if (curLeft == curLeftPositionCount) {
        /** current leftPattern has reached end */
        if (leftIndex + 1 < leftPatternCount) {
          /** read next rlePattern */
          curLeft = 0;
          leftIndex++;
          leftPatternColumn = ((RLEColumn) leftColumn).getColumn(leftIndex);
          curLeftPositionCount = ((RLEColumn) leftColumn).getLogicPositionCount(leftIndex);
          isRLELeft = leftPatternColumn.getPositionCount() == 1;
        } else {
          /** leftRLEColumn has reached end, if rightRLEColumn didn't reach end, something wrong. */
          if (rightIndex < rightPatternCount - 1) {
            throw new RuntimeException("leftColumn and rightColumn have unequal length");
          } else {
            break;
          }
        }
      }

      if (curRight == curRightPositionCount) {
        /** current rightPattern has reached end */
        if (rightIndex + 1 < rightPatternCount) {
          /** read next rlePattern */
          curRight = 0;
          rightIndex++;
          rightPatternColumn = ((RLEColumn) rightColumn).getColumn(rightIndex);
          curRightPositionCount = ((RLEColumn) rightColumn).getLogicPositionCount(rightIndex);
          isRLERight = rightPatternColumn.getPositionCount() == 1;
        } else {
          /** rightRLEColumn has reached end, if leftRLEColumn didn't reach end, something wrong. */
          if (leftIndex < leftPatternCount - 1) {
            throw new RuntimeException("leftColumn and rightColumn have unequal length");
          } else {
            break;
          }
        }
      }
      length =
          curLeftPositionCount - curLeft > curRightPositionCount - curRight
              ? curRightPositionCount - curRight
              : curLeftPositionCount - curLeft;
      length = length > positionCount - index ? positionCount - index : length;

      if (isRLELeft && isRLERight) {
        double res =
            transform(
                leftTransformer.getType().getDouble(leftPatternColumn, 0),
                rightTransformer.getType().getDouble(rightPatternColumn, 0));
        for (int i = 0; i < length; i++) {
          returnType.writeDouble(builder, res);
        }
        index += length;
        curLeft += length;
        curRight += length;
      } else if (isRLELeft) {
        double leftValue = leftTransformer.getType().getDouble(leftPatternColumn, 0);
        for (int i = 0; i < length; i++, curRight++, index++) {
          if (!rightPatternColumn.isNull(curRight)) {
            returnType.writeDouble(
                builder,
                transform(
                    leftValue, rightTransformer.getType().getDouble(rightPatternColumn, curRight)));
          } else {
            builder.appendNull();
          }
        }
        curLeft += length;
      } else if (isRLERight) {
        double rightValue = rightTransformer.getType().getDouble(rightPatternColumn, 0);
        for (int i = 0; i < length; i++, curLeft++, index++) {
          if (!leftPatternColumn.isNull(curLeft)) {
            returnType.writeDouble(
                builder,
                transform(
                    leftTransformer.getType().getDouble(leftPatternColumn, curLeft), rightValue));
          } else {
            builder.appendNull();
          }
        }
        curRight += length;
      } else {
        for (int i = 0; i < length; i++, curLeft++, curRight++, index++) {
          if (!leftPatternColumn.isNull(curLeft) && !rightPatternColumn.isNull(curRight)) {
            returnType.writeDouble(
                builder,
                transform(
                    leftTransformer.getType().getDouble(leftPatternColumn, curLeft),
                    rightTransformer.getType().getDouble(rightPatternColumn, curRight)));
          } else {
            builder.appendNull();
          }
        }
      }
    }
  }

  private void doTransformWithLeftRLEandConstant(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    int leftPatternCount = ((RLEColumn) leftColumn).getPatternCount();
    int leftIndex = 0;
    int curLeft = 0;
    Column leftPatternColumn = ((RLEColumn) leftColumn).getColumn(leftIndex);
    int curLeftPositionCount = ((RLEColumn) leftColumn).getLogicPositionCount(leftIndex);
    int index = 0;
    int length = 0;
    double value = rightTransformer.getType().getDouble(rightColumn, 0);
    while (index < positionCount) {
      if (curLeft == curLeftPositionCount) {
        /** current leftPattern has reached end */
        if (leftIndex + 1 < leftPatternCount) {
          /** read next rlePattern */
          curLeft = 0;
          leftIndex++;
          leftPatternColumn = ((RLEColumn) leftColumn).getColumn(leftIndex);
          curLeftPositionCount = ((RLEColumn) leftColumn).getLogicPositionCount(leftIndex);
        } else {
          throw new RuntimeException(
              "The positionCount of leftColumn is less than the requested positionCount");
        }
      }
      length =
          curLeftPositionCount - curLeft > positionCount - index
              ? positionCount - index
              : curLeftPositionCount - curLeft;
      if (leftPatternColumn.getPositionCount() == 1) {
        double res = transform(leftTransformer.getType().getDouble(leftPatternColumn, 0), value);
        for (int i = 0; i < length; i++) {
          returnType.writeDouble(builder, res);
        }
        index += length;
        curLeft += length;
      } else {
        for (int i = 0; i < length; i++, curLeft++, index++) {
          if (!leftPatternColumn.isNull(curLeft)) {
            returnType.writeDouble(
                builder,
                transform(leftTransformer.getType().getDouble(leftPatternColumn, curLeft), value));
          } else {
            builder.appendNull();
          }
        }
      }
    }
  }

  private void doTransformWithRightRLEandConstant(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    int rightPatternCount = ((RLEColumn) rightColumn).getPatternCount();
    int rightIndex = 0;
    int curRight = 0;
    Column rightPatternColumn = ((RLEColumn) rightColumn).getColumn(rightIndex);
    int curRightPositionCount = ((RLEColumn) rightColumn).getLogicPositionCount(rightIndex);
    int index = 0;
    int length = 0;
    ((RLEColumn) rightColumn).print();
    double value = leftTransformer.getType().getDouble(leftColumn, 0);
    while (index < positionCount) {
      if (curRight == curRightPositionCount) {
        /** current rightPattern has reached end */
        if (rightIndex + 1 < rightPatternCount) {
          /** read next rlePattern */
          curRight = 0;
          rightIndex++;
          rightPatternColumn = ((RLEColumn) rightColumn).getColumn(rightIndex);
          curRightPositionCount = ((RLEColumn) rightColumn).getLogicPositionCount(rightIndex);
        } else {
          throw new RuntimeException(
              "The positionCount of rightColumn is less than the requested positionCount");
        }
      }

      length =
          curRightPositionCount - curRight > positionCount - index
              ? positionCount - index
              : curRightPositionCount - curRight;
      if (rightPatternColumn.getPositionCount() == 1) {
        double res = transform(value, rightTransformer.getType().getDouble(rightPatternColumn, 0));
        for (int i = 0; i < length; i++) {
          returnType.writeDouble(builder, res);
        }
        index += length;
        curRight += length;
      } else {
        for (int i = 0; i < length; i++, curRight++, index++) {
          if (!rightPatternColumn.isNull(curRight)) {
            returnType.writeDouble(
                builder,
                transform(
                    value, rightTransformer.getType().getDouble(rightPatternColumn, curRight)));
          } else {
            builder.appendNull();
          }
        }
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    if (leftColumn instanceof RLEColumn && rightColumn instanceof RLEColumn) {
      doTransformBetweenRLE(leftColumn, rightColumn, builder, positionCount);
      return;
    } else if (leftColumn instanceof RLEColumn && rightColumn instanceof RunLengthEncodedColumn) {
      doTransformWithLeftRLEandConstant(leftColumn, rightColumn, builder, positionCount);
      return;
    } else if (rightColumn instanceof RLEColumn && leftColumn instanceof RunLengthEncodedColumn) {
      doTransformWithRightRLEandConstant(leftColumn, rightColumn, builder, positionCount);
      return;
    }

    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.writeDouble(
            builder,
            transform(
                leftTransformer.getType().getDouble(leftColumn, i),
                rightTransformer.getType().getDouble(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    if (!leftTransformer.isReturnTypeNumeric() || !rightTransformer.isReturnTypeNumeric()) {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  protected abstract double transform(double d1, double d2);
}
