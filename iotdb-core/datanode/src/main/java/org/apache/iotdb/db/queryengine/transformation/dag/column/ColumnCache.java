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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

public class ColumnCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnCache.class);
  private int referenceCount;
  private Column column;

  public ColumnCache() {
    // do nothing
  }

  public Column getColumn() {
    referenceCount--;
    if (column != null) {
      LOGGER.warn("referenceCount: {}, column: {}, {}", referenceCount, column, column.getTsPrimitiveType(0));
    } else {
      LOGGER.warn("referenceCount: {}, column is null!", referenceCount);
    }
    checkArgument(referenceCount >= 0, "Exceed max call times of getColumn");
    Column res = this.column;
    // set column to null for memory control
    if (referenceCount == 0) {
      this.column = null;
    }
    return res;
  }

  public int getPositionCount() {
    return column != null ? column.getPositionCount() : 0;
  }

  public void cacheColumn(Column column, int referenceCount) {
    this.referenceCount = referenceCount;
    this.column = column;
  }

  public boolean hasCached() {
    return column != null;
  }
}
