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
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.type.RlePatternType;
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
    int leftPositionCount = leftColumn.getPositionCount();
    int rightPositionCount = rightColumn.getPositionCount();
    int leftIndex = 0, rightIndex = 0;
    int curLeft = 0, curRight = 0;
    RLEPatternColumn leftPatternColumn = ((RLEColumn) leftColumn).getRLEPattern(leftIndex);
    RLEPatternColumn rightPatternColumn = ((RLEColumn) rightColumn).getRLEPattern(rightIndex);
    boolean isRLELeft = leftPatternColumn.isRLEMode();
    boolean isRLERight = rightPatternColumn.isRLEMode();
    int curLeftPositionCount = leftPatternColumn.getPositionCount();
    int curRightPositionCount = rightPatternColumn.getPositionCount();
    int index = 0;
    int length = 0;
    while (index < positionCount) {
      if (curLeft == curLeftPositionCount) {
        /** current leftPattern has reached end */
        if (leftIndex + 1 < leftPositionCount) {
          /** read next rlePattern */
          curLeft = 0;
          leftIndex++;
          leftPatternColumn = ((RLEColumn) leftColumn).getRLEPattern(leftIndex);
          curLeftPositionCount = leftPatternColumn.getPositionCount();
          isRLELeft = leftPatternColumn.isRLEMode();
        } else {
          /** leftRLEColumn has reached end, if rightRLEColumn didn't reach end, something wrong. */
          if (rightIndex < rightPositionCount - 1) {
            throw new RuntimeException("leftColumn and rightColumn have unequal length");
          } else {
            break;
          }
        }
      }

      if (curRight == curRightPositionCount) {
        /** current rightPattern has reached end */
        if (rightIndex + 1 < rightPositionCount) {
          /** read next rlePattern */
          curRight = 0;
          rightIndex++;
          rightPatternColumn = ((RLEColumn) rightColumn).getRLEPattern(rightIndex);
          curRightPositionCount = rightPatternColumn.getPositionCount();
          isRLERight = rightPatternColumn.isRLEMode();
        } else {
          /** rightRLEColumn has reached end, if leftRLEColumn didn't reach end, something wrong. */
          if (leftIndex < leftPositionCount - 1) {
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
                RlePatternType.getInstance().getDouble(leftPatternColumn, 0),
                RlePatternType.getInstance().getDouble(rightPatternColumn, 0));
        for (int i = 0; i < length; i++) {
          returnType.writeDouble(builder, res);
        }
        index += length;
        curLeft += length;
        curRight += length;
      } else if (isRLELeft) {
        double leftValue = RlePatternType.getInstance().getDouble(leftPatternColumn, 0);
        for (int i = 0; i < length; i++, curRight++, index++) {
          if (!rightPatternColumn.isNull(curRight)) {
            returnType.writeDouble(
                builder,
                transform(
                    leftValue,
                    RlePatternType.getInstance().getDouble(rightPatternColumn, curRight)));
          } else {
            builder.appendNull();
          }
        }
        curLeft += length;
      } else if (isRLERight) {
        double rightValue = RlePatternType.getInstance().getDouble(rightPatternColumn, 0);
        for (int i = 0; i < length; i++, curLeft++, index++) {
          if (!leftPatternColumn.isNull(curLeft)) {
            returnType.writeDouble(
                builder,
                transform(
                    RlePatternType.getInstance().getDouble(leftPatternColumn, curLeft),
                    rightValue));
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
                    RlePatternType.getInstance().getDouble(leftPatternColumn, curLeft),
                    RlePatternType.getInstance().getDouble(rightPatternColumn, curRight)));
          } else {
            builder.appendNull();
          }
        }
      }
    }
  }

  private void doTransformWithLeftRLE(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    int leftPositionCount = leftColumn.getPositionCount();
    int leftIndex = 0;
    int curLeft = 0;
    RLEPatternColumn leftPatternColumn = ((RLEColumn) leftColumn).getRLEPattern(leftIndex);
    boolean isRLELeft = leftPatternColumn.isRLEMode();
    int curLeftPositionCount = leftPatternColumn.getPositionCount();
    int index = 0;
    int length = 0;

    if (!(leftColumn instanceof RLEColumn)) {
      throw new IllegalArgumentException("left Column is not a RLEColumn, RLEColumn expected.");
    }
    if (rightColumn instanceof RunLengthEncodedColumn) {
      double value = rightTransformer.getType().getDouble(rightColumn, 0);
      while (index < positionCount) {
        if (curLeft == curLeftPositionCount) {
          /** current leftPattern has reached end */
          if (leftIndex + 1 < leftPositionCount) {
            /** read next rlePattern */
            curLeft = 0;
            leftIndex++;
            leftPatternColumn = ((RLEColumn) leftColumn).getRLEPattern(leftIndex);
            curLeftPositionCount = leftPatternColumn.getPositionCount();
            isRLELeft = leftPatternColumn.isRLEMode();
          } else {
            break;
          }
        }
        length =
            curLeftPositionCount - curLeft > positionCount - index
                ? positionCount - index
                : curLeftPositionCount - curLeft;
        if (isRLELeft) {
          double res =
              transform(RlePatternType.getInstance().getDouble(leftPatternColumn, 0), value);
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
                  transform(
                      RlePatternType.getInstance().getDouble(leftPatternColumn, curLeft), value));
            } else {
              builder.appendNull();
            }
          }
        }
      }
    } else {
      while (index < positionCount) {
        if (curLeft == curLeftPositionCount) {
          /** current leftPattern has reached end */
          if (leftIndex + 1 < leftPositionCount) {
            /** read next rlePattern */
            curLeft = 0;
            leftIndex++;
            leftPatternColumn = ((RLEColumn) leftColumn).getRLEPattern(leftIndex);
            curLeftPositionCount = leftPatternColumn.getPositionCount();
            isRLELeft = leftPatternColumn.isRLEMode();
          } else {
            break;
          }
        }
        length =
            curLeftPositionCount - curLeft > positionCount - index
                ? positionCount - index
                : curLeftPositionCount - curLeft;
        if (isRLELeft) {
          double leftValue = RlePatternType.getInstance().getDouble(leftPatternColumn, 0);
          for (int i = 0; i < length; i++, index++) {
            if (!rightColumn.isNull(index)) {
              returnType.writeDouble(
                  builder,
                  transform(leftValue, rightTransformer.getType().getDouble(rightColumn, index)));
            } else {
              builder.appendNull();
            }
          }
          curLeft += length;
        } else {
          for (int i = 0; i < length; i++, curLeft++, index++) {
            if (!rightColumn.isNull(index) && !leftPatternColumn.isNull(curLeft)) {
              returnType.writeDouble(
                  builder,
                  transform(
                      RlePatternType.getInstance().getDouble(leftPatternColumn, curLeft),
                      rightTransformer.getType().getDouble(rightColumn, index)));
            } else {
              builder.appendNull();
            }
          }
        }
      }
    }
  }

  private void doTransformWithRightRLE(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    int rightPositionCount = rightColumn.getPositionCount();
    int rightIndex = 0;
    int curRight = 0;
    RLEPatternColumn rightPatternColumn = ((RLEColumn) rightColumn).getRLEPattern(rightIndex);
    boolean isRLERight = rightPatternColumn.isRLEMode();
    int curRightPositionCount = rightPatternColumn.getPositionCount();
    int index = 0;
    int length = 0;

    if (!(rightColumn instanceof RLEColumn)) {
      throw new IllegalArgumentException("right Column is not a RLEColumn, RLEColumn expected.");
    }
    if (leftColumn instanceof RunLengthEncodedColumn) {
      double value = leftTransformer.getType().getDouble(leftColumn, 0);
      while (index < positionCount) {
        if (curRight == curRightPositionCount) {
          /** current rightPattern has reached end */
          if (rightIndex + 1 < rightPositionCount) {
            /** read next rlePattern */
            curRight = 0;
            rightIndex++;
            rightPatternColumn = ((RLEColumn) rightColumn).getRLEPattern(rightIndex);
            curRightPositionCount = rightPatternColumn.getPositionCount();
            isRLERight = rightPatternColumn.isRLEMode();
          } else {
            break;
          }
        }

        length =
            curRightPositionCount - curRight > positionCount - index
                ? positionCount - index
                : curRightPositionCount - curRight;
        if (isRLERight) {
          double res =
              transform(value, RlePatternType.getInstance().getDouble(rightPatternColumn, 0));
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
                      value, RlePatternType.getInstance().getDouble(rightPatternColumn, curRight)));
            } else {
              builder.appendNull();
            }
          }
        }
      }
    } else {
      while (index < positionCount) {
        if (curRight == curRightPositionCount) {
          /** current rightPattern has reached end */
          if (rightIndex + 1 < rightPositionCount) {
            /** read next rlePattern */
            curRight = 0;
            rightIndex++;
            rightPatternColumn = ((RLEColumn) rightColumn).getRLEPattern(rightIndex);
            curRightPositionCount = rightPatternColumn.getPositionCount();
            isRLERight = rightPatternColumn.isRLEMode();
          } else {
            break;
          }
        }
        length =
            curRightPositionCount - curRight > positionCount - index
                ? positionCount - index
                : curRightPositionCount - curRight;
        if (isRLERight) {
          double rightValue = RlePatternType.getInstance().getDouble(rightPatternColumn, 0);
          for (int i = 0; i < length; i++, index++) {
            if (!leftColumn.isNull(index)) {
              returnType.writeDouble(
                  builder,
                  transform(leftTransformer.getType().getDouble(leftColumn, index), rightValue));
            } else {
              builder.appendNull();
            }
          }
          curRight += length;
        } else {
          for (int i = 0; i < length; i++, curRight++, index++) {
            if (!rightPatternColumn.isNull(curRight) && !leftColumn.isNull(index)) {
              returnType.writeDouble(
                  builder,
                  transform(
                      leftTransformer.getType().getDouble(leftColumn, index),
                      RlePatternType.getInstance().getDouble(rightPatternColumn, curRight)));
            } else {
              builder.appendNull();
            }
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
    } else if (leftColumn instanceof RLEColumn) {
      doTransformWithLeftRLE(leftColumn, rightColumn, builder, positionCount);
      return;
    } else if (rightColumn instanceof RLEColumn) {
      doTransformWithRightRLE(leftColumn, rightColumn, builder, positionCount);
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
