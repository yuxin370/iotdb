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

package org.apache.iotdb.db.queryengine.transformation.datastructure;

import org.apache.iotdb.db.service.TemporaryQueryDataFileService;

public abstract class SerializableListTest {

  protected static final int ITERATION_TIMES =
      10000; // make sure serialize() & deserialize() can be triggered

  protected static final float MEMORY_USAGE_LIMIT_IN_MB = 100f;
  protected static final int CACHE_SIZE = 3;

  protected static final String QUERY_ID = String.valueOf(0);

  protected static final int INTERNAL_ROW_RECORD_LIST_CAPACITY = 8;

  public void setUp() throws Exception {
    TemporaryQueryDataFileService.getInstance().start();
  }

  public void tearDown() {
    TemporaryQueryDataFileService.getInstance().stop();
  }
}
