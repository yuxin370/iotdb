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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Decoder for int value using rle or bit-packing. */
public class IntRleDecoder extends RleDecoder {

  private static final Logger logger = LoggerFactory.getLogger(IntRleDecoder.class);

  /** current value for rle repeated value. */
  private int currentValue;

  /** buffer to save all values in group using bit-packing. */
  private int[] currentBuffer;

  /** packer for unpacking int values. */
  private IntPacker packer;

  public IntRleDecoder() {
    super();
    currentValue = 0;
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    return this.readInt(buffer) == 0 ? false : true;
  }

  /**
   * read an int value from InputStream.
   *
   * @param buffer - ByteBuffer
   * @return value - current valid value
   */
  @Override
  public int readInt(ByteBuffer buffer) {
    if (!isLengthAndBitWidthReaded) {
      // start to read a new rle+bit-packing pattern
      readLengthAndBitWidth(buffer);
    }

    if (currentCount == 0) {
      try {
        readNext();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                + " length is {}, bit width is {}",
            length,
            bitWidth,
            e);
      }
    }
    --currentCount;
    int result;
    switch (mode) {
      case RLE:
        result = currentValue;
        break;
      case BIT_PACKED:
        result = currentBuffer[bitPackingNum - currentCount - 1];
        break;
      default:
        throw new TsFileDecodingException(
            String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
    }

    if (!hasNextPackage()) {
      isLengthAndBitWidthReaded = false;
    }
    return result;
  }

  public RLEPatternColumn readRlePatternInt(ByteBuffer buffer) {
    IntColumnBuilder builder = new IntColumnBuilder(null, 0);
    if (!isLengthAndBitWidthReaded) {
      // start to read a new rle+bit-packing pattern
      readLengthAndBitWidth(buffer);
    }

    if (currentCount == 0) {
      try {
        readNext();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                + " length is {}, bit width is {}",
            length,
            bitWidth,
            e);
      }
    }
    // --currentCount;
    int valueCount = currentCount;
    switch (mode) {
      case RLE:
        builder.writeInt(currentValue);
        currentCount = 0;
        break;
      case BIT_PACKED:
        while (currentCount != 0) {
          currentCount--;
          builder.writeInt(currentBuffer[bitPackingNum - currentCount - 1]);
        }
        break;
      default:
        throw new TsFileDecodingException(
            String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
    }

    if (!hasNextPackage()) {
      isLengthAndBitWidthReaded = false;
    }
    return new RLEPatternColumn(builder.build(), valueCount, mode == Mode.RLE ? 0 : 1);
  }

  public RLEPatternColumn readRlePatternBoolean(ByteBuffer buffer) {
    BooleanColumnBuilder builder = new BooleanColumnBuilder(null, 0);
    if (!isLengthAndBitWidthReaded) {
      // start to read a new rle+bit-packing pattern
      readLengthAndBitWidth(buffer);
    }

    if (currentCount == 0) {
      try {
        readNext();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                + " length is {}, bit width is {}",
            length,
            bitWidth,
            e);
      }
    }
    // --currentCount;
    int valueCount = currentCount;
    switch (mode) {
      case RLE:
        builder.writeBoolean(currentValue == 0 ? false : true);
        currentCount = 0;
        break;
      case BIT_PACKED:
        while (currentCount != 0) {
          currentCount--;
          builder.writeBoolean(currentBuffer[bitPackingNum - currentCount - 1] == 0 ? false : true);
        }
        break;
      default:
        throw new TsFileDecodingException(
            String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
    }

    if (!hasNextPackage()) {
      isLengthAndBitWidthReaded = false;
    }
    return new RLEPatternColumn(builder.build(), valueCount, mode == Mode.RLE ? 0 : 1);
  }

  public RLEPatternColumn readRlePatternFloat(ByteBuffer buffer) {
    FloatColumnBuilder builder = new FloatColumnBuilder(null, 0);
    if (!isLengthAndBitWidthReaded) {
      // start to read a new rle+bit-packing pattern
      readLengthAndBitWidth(buffer);
    }

    if (currentCount == 0) {
      try {
        readNext();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                + " length is {}, bit width is {}",
            length,
            bitWidth,
            e);
      }
    }
    // --currentCount;
    int valueCount = currentCount;
    switch (mode) {
      case RLE:
        builder.writeFloat(currentValue);
        currentCount = 0;
        break;
      case BIT_PACKED:
        while (currentCount != 0) {
          currentCount--;
          builder.writeFloat(currentBuffer[bitPackingNum - currentCount - 1]);
        }
        break;
      default:
        throw new TsFileDecodingException(
            String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
    }

    if (!hasNextPackage()) {
      isLengthAndBitWidthReaded = false;
    }
    return new RLEPatternColumn(builder.build(), valueCount, mode == Mode.RLE ? 0 : 1);
  }

  /**
   * read an RLEPattern value from InputStream.
   *
   * @param buffer - ByteBuffer
   * @return value - current valid RLEPattern value
   */
  @Override
  public RLEPatternColumn readRLEPattern(ByteBuffer buffer, TSDataType dataType) {
    switch (dataType) {
      case INT32:
        return readRlePatternInt(buffer);
      case FLOAT:
        return readRlePatternFloat(buffer);
      case BOOLEAN:
        return readRlePatternBoolean(buffer);
      default:
        throw new IllegalArgumentException(
            "IntRleDecoder only support dataType [INT32,FLOAT,BOLEAN], but get a "
                + dataType
                + ".");
    }
  }

  @Override
  protected void initPacker() {
    packer = new IntPacker(bitWidth);
  }

  @Override
  protected void readNumberInRle() throws IOException {
    currentValue =
        ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(byteCache, bitWidth);
  }

  @Override
  protected void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum) {
    currentBuffer = new int[bitPackedGroupCount * TSFileConfig.RLE_MIN_REPEATED_NUM];
    byte[] bytes = new byte[bitPackedGroupCount * bitWidth];
    int bytesToRead = bitPackedGroupCount * bitWidth;
    bytesToRead = Math.min(bytesToRead, byteCache.remaining());
    byteCache.get(bytes, 0, bytesToRead);

    // save all int values in currentBuffer
    packer.unpackAllValues(bytes, bytesToRead, currentBuffer);
  }
}
