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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public class ArithmeticNegationColumnTransformer extends UnaryColumnTransformer {
  public ArithmeticNegationColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    if (column instanceof RLEColumn) {
      RLEColumn rleColumn = (RLEColumn) column;
      if (!(columnBuilder instanceof RLEColumnBuilder)) {
        columnBuilder =
            new RLEColumnBuilder(null, rleColumn.getPatternCount(), returnType.getTypeEnum());
      }
      for (int i = 0, n = rleColumn.getPatternCount(); i < n; i++) {
        Column aColumn = rleColumn.getColumn(i);
        int logicPositionCount = rleColumn.getLogicPositionCount(i);
        if (aColumn.getPositionCount() == 1) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          if (!aColumn.isNull(0)) {
            returnType.writeDouble(
                columnBuilderTmp, -childColumnTransformer.getType().getDouble(aColumn, 0));
          } else {
            columnBuilderTmp.appendNull();
          }
          columnBuilder.writeColumn(columnBuilderTmp.build(), logicPositionCount);
        } else {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(logicPositionCount);
          for (int j = 0; j < logicPositionCount; j++) {
            if (!aColumn.isNull(j)) {
              returnType.writeDouble(
                  columnBuilderTmp, -childColumnTransformer.getType().getDouble(aColumn, j));
            } else {
              columnBuilderTmp.appendNull();
            }
          }
          columnBuilder.writeColumn(columnBuilderTmp.build(), logicPositionCount);
        }
      }
      return;
    }
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        returnType.writeDouble(
            columnBuilder, -childColumnTransformer.getType().getDouble(column, i));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected final void checkType() {
    if (!childColumnTransformer.isReturnTypeNumeric()) {
      throw new UnsupportedOperationException("Unsupported Type: " + returnType.toString());
    }
  }
}
