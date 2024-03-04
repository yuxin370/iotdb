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
package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.db.queryengine.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GroupByCountParameter extends GroupByParameter {

  private final long countNumber;
  private final boolean ignoreNull;

  public GroupByCountParameter(long countNumber, boolean ignoreNull) {
    super(WindowType.COUNT_WINDOW);
    this.countNumber = countNumber;
    this.ignoreNull = ignoreNull;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(countNumber, byteBuffer);
    ReadWriteIOUtils.write(ignoreNull, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(countNumber, stream);
    ReadWriteIOUtils.write(ignoreNull, stream);
  }

  public long getCountNumber() {
    return countNumber;
  }

  public boolean isIgnoreNull() {
    return ignoreNull;
  }

  public static GroupByParameter deserialize(ByteBuffer buffer) {
    long countNumber = ReadWriteIOUtils.readLong(buffer);
    boolean ignoreNull = ReadWriteIOUtils.readBool(buffer);
    return new GroupByCountParameter(countNumber, ignoreNull);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    return this.countNumber == ((GroupByCountParameter) obj).getCountNumber()
        || this.ignoreNull == ((GroupByCountParameter) obj).isIgnoreNull();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), countNumber, ignoreNull);
  }
}
