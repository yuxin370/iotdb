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
import org.apache.iotdb.tsfile.utils.RLEPattern;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RLEColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // Serialized data layout:
    //
    // +----------+---------------+--------------+-------------------------+--------------------------+
    // | encoding | pattern count | offset index | physical positionCounts | serialized inner
    // columns |
    // +----------+---------------+--------------+-------------------------+--------------------------+
    // | byte     | int           | list[int]    |  list[int]              | list[bytes]
    //  |
    // +----------+---------------+--------------+-------------------------+--------------------------+
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(ColumnEncoding.deserializeFrom(input));
    int patternCount = input.getInt();
    int[] patternOffsetIndex = new int[patternCount + 1];
    int[] physicalPositionCount = new int[patternCount];
    patternOffsetIndex[0] = 0;
    for (int i = 1; i <= patternCount; i++) {
      patternOffsetIndex[i] = input.getInt();
    }
    for (int i = 0; i < patternCount; i++) {
      physicalPositionCount[i] = input.getInt();
    }

    RLEPattern[] values = new RLEPattern[patternCount];
    for (int i = 0; i < patternCount; i++) {
      RLEPattern tmp =
          new RLEPattern(
              columnEncoder.readColumn(input, dataType, physicalPositionCount[i]),
              patternOffsetIndex[i + 1] - patternOffsetIndex[i]);
      values[i] = tmp;
    }

    return new RLEColumn(positionCount, patternCount, values, patternOffsetIndex);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {
    if (!(column instanceof RLEColumn)) {
      throw new IllegalArgumentException("Unable to write column that not a RLEColumn");
    }

    RLEColumn RleColumn = (RLEColumn) column;
    int patternCount = RleColumn.getPatternCount();

    RleColumn.getColumn(0).getEncoding().serializeTo(output);
    output.writeInt(patternCount);
    for (int i = 1; i <= patternCount; i++) {
      output.writeInt(RleColumn.getPatternOffsetIndex(i));
    }
    for (int i = 0; i < patternCount; i++) {
      output.writeInt(RleColumn.getColumn(i).getPositionCount());
    }

    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(RleColumn.getColumn(0).getEncoding());
    for (int i = 0; i < patternCount; i++) {
      columnEncoder.writeColumn(output, RleColumn.getColumn(i));
    }
  }
}
