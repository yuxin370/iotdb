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

package org.apache.iotdb.tsfile.read.common.type;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

public class RlePatternType implements Type {
  /** used to get RlePattern value with different datatype throw getObject api */
  private static final RlePatternType INSTANCE = new RlePatternType();

  private RlePatternType() {}

  @Override
  public int getInt(Column c, int position) {
    switch (c.getDataType()) {
      case INT32:
        return c.getInt(position);
      case INT64:
        return (int) c.getLong(position);
      case DOUBLE:
        return (int) c.getDouble(position);
      case FLOAT:
        return c.getInt(position);
      default:
        throw new UnSupportedDataTypeException(
            "unsuported dataType" + c.getDataType() + " for getInt()");
    }
  }

  @Override
  public long getLong(Column c, int position) {
    switch (c.getDataType()) {
      case INT32:
        return c.getInt(position);
      case INT64:
        return c.getLong(position);
      case DOUBLE:
        return (long) c.getDouble(position);
      case FLOAT:
        return c.getInt(position);
      default:
        throw new UnSupportedDataTypeException(
            "unsuported dataType" + c.getDataType() + " for getLong()");
    }
  }

  @Override
  public float getFloat(Column c, int position) {
    switch (c.getDataType()) {
      case INT32:
        return c.getInt(position);
      case INT64:
        return c.getLong(position);
      case DOUBLE:
        return (float) c.getDouble(position);
      case FLOAT:
        return c.getInt(position);
      default:
        throw new UnSupportedDataTypeException(
            "unsuported dataType" + c.getDataType() + " for getFloat()");
    }
  }

  @Override
  public double getDouble(Column c, int position) {
    switch (c.getDataType()) {
      case INT32:
        return c.getInt(position);
      case INT64:
        return c.getLong(position);
      case DOUBLE:
        return c.getDouble(position);
      case FLOAT:
        return c.getInt(position);
      default:
        throw new UnSupportedDataTypeException(
            "unsuported dataType" + c.getDataType() + " for getDouble()");
    }
  }

  @Override
  public boolean getBoolean(Column c, int position) {
    if (c.getDataType() == TSDataType.BOOLEAN) {
      return c.getBoolean(position);
    } else {
      throw new UnSupportedDataTypeException(
          "unsuported dataType" + c.getDataType() + " for getBoolean()");
    }
  }

  @Override
  public Binary getBinary(Column c, int position) {
    if (c.getDataType() == TSDataType.TEXT) {
      return c.getBinary(position);
    } else {
      throw new UnSupportedDataTypeException(
          "unsuported dataType" + c.getDataType() + " for getBinary()");
    }
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public void writeBoolean(ColumnBuilder builder, boolean value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public void writeBinary(ColumnBuilder builder, Binary value) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    throw new UnsupportedOperationException("RLEPatternColumn has no builder");
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.RLEPATTERN;
  }

  public static RlePatternType getInstance() {
    return INSTANCE;
  }
}
