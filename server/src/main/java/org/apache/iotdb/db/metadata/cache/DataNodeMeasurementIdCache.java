/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class DataNodeMeasurementIdCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<String, byte[]> measurementIdCache;

  private DataNodeMeasurementIdCache() {
    measurementIdCache = Caffeine.newBuilder().maximumSize(config.getDevicePathCacheSize()).build();
  }

  public static DataNodeMeasurementIdCache getInstance() {
    return DataNodeMeasurementIdCache.DataNodeMeasurementIdCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeMeasurementIdCacheHolder {
    private static final DataNodeMeasurementIdCache INSTANCE = new DataNodeMeasurementIdCache();
  }

  public boolean contains(String measurementId) {
    return null != measurementIdCache.get(measurementId, k -> null);
  }

  public void put(String measurementId) {
    measurementIdCache.put(measurementId, measurementId.getBytes());
  }

  public byte[] getBytes(String measurementId) {
    return measurementIdCache.get(measurementId, String::getBytes);
  }

  public void cleanUp() {
    measurementIdCache.cleanUp();
  }
}
