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
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn.RunLengthMode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RLEPatternColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // Serialized data layout:
    //    +----------+------------------+------+-----------------+------------------------+
    //    | encoding |  value data type | mode | positioncount   |serialized inner column |
    //    +----------+------------------+------+-----------------+------------------------+
    //    | byte     |     byte         | byte |   int           |  list[byte]            |
    //    +----------+------------------+------+-----------------+------------------------+
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(ColumnEncoding.deserializeFrom(input));
    TSDataType readDataType = TSDataType.deserializeFrom(input);
    RunLengthMode mode = RunLengthMode.deserializeFrom(input);
    Column innerColumn;
    int totalPositionCount = input.getInt();
    positionCount = totalPositionCount > positionCount ? positionCount : totalPositionCount;
    if (mode == RunLengthMode.RLE) {
      innerColumn = columnEncoder.readColumn(input, readDataType, 1);
    } else {
      innerColumn = columnEncoder.readColumn(input, readDataType, positionCount);
    }
    return new RLEPatternColumn(innerColumn, positionCount, mode);
  }

  public Column readColumn(ByteBuffer input, TSDataType dataType) {
    // Serialized data layout:
    //    +----------+------------------+------+-----------------+------------------------+
    //    | encoding |  value data type | mode | positioncount   |serialized inner column |
    //    +----------+------------------+------+-----------------+------------------------+
    //    | byte     |     byte         | byte |   int           |  list[byte]            |
    //    +----------+------------------+------+-----------------+------------------------+
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(ColumnEncoding.deserializeFrom(input));
    TSDataType readDataType = TSDataType.deserializeFrom(input);
    RunLengthMode mode = RunLengthMode.deserializeFrom(input);
    Column innerColumn;
    int positionCount = input.getInt();
    if (mode == RunLengthMode.RLE) {
      innerColumn = columnEncoder.readColumn(input, readDataType, 1);
    } else {
      innerColumn = columnEncoder.readColumn(input, readDataType, positionCount);
    }
    return new RLEPatternColumn(innerColumn, positionCount, mode);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {
    Column innerColumn = ((RLEPatternColumn) column).getValue();
    RunLengthMode mode = ((RLEPatternColumn) column).getMode();
    int positionCount = ((RLEPatternColumn) column).getPositionCount();
    TSDataType dataType = ((RLEPatternColumn) column).getDataType();
    if (innerColumn instanceof RLEPatternColumn) {
      throw new IOException("Unable to encode a nested RLE column.");
    }

    innerColumn.getEncoding().serializeTo(output);
    dataType.serializeTo(output);
    mode.serializeTo(output);
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(innerColumn.getEncoding());
    output.writeInt(positionCount);
    columnEncoder.writeColumn(output, innerColumn);
  }
}
