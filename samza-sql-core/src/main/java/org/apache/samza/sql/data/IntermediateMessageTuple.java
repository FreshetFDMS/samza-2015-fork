/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.data;

import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.system.sql.Offset;

/**
 * Defines a tuple passed between operators in a query plan.
 */
public class IntermediateMessageTuple implements Tuple {
  private final Data message;
  private final boolean delete;
  private final Data key;
  private final EntityName streamName;
  private final long createTimeNano;
  private final Offset offset;

  /**
   * Constructor for {@link IntermediateMessageTuple}
   *
   * @param message    actual value of this tuple
   * @param delete     whether this tuple indicates a deletion from time-varying relation
   * @param key        unique key of the tuple
   * @param entityName stream name corresponding to time-varying relation this tuple belongs to
   * @param createTimeNano creation time of the message
   * @param offset         offset of the message
   */
  public IntermediateMessageTuple(Data message, boolean delete, Data key, EntityName entityName, long createTimeNano, Offset offset) {
    this.message = message;
    this.delete = delete;
    this.key = key;
    this.streamName = entityName;
    this.createTimeNano = createTimeNano;
    this.offset = offset;
  }

  @Override
  public Data getMessage() {
    return message;
  }

  @Override
  public boolean isDelete() {
    return delete;
  }

  @Override
  public Data getKey() {
    return key;
  }

  @Override
  public EntityName getEntityName() {
    return streamName;
  }

  @Override
  public long getCreateTimeNano() {
    return createTimeNano;
  }

  @Override
  public Offset getOffset() {
    return offset;
  }

  /**
   * Creates instance of {@link IntermediateMessageTuple} from a tuple.
   *
   * @param t          original tuple
   * @param delete     is this tuple indicates a deletion from time-varying relation
   * @param streamName stream name corresponding to time-varying relation this tuple belongs to
   * @return instance of {@link IntermediateMessageTuple}
   */
  public static IntermediateMessageTuple fromTuple(Tuple t, boolean delete, EntityName streamName) {
    return new IntermediateMessageTuple(t.getMessage(), delete, t.getKey(), streamName, t.getCreateTimeNano(), t.getOffset());
  }

  /**
   * Creates instance of {@link IntermediateMessageTuple} from a message and related data.
   *
   * @param message          message
   * @param delete           is this tuple indicates a deletion from time-varying relation
   * @param streamName       stream name corresponding to time-varying relation this tuple belongs to
   * @param key              unique key of the tuple
   * @param createTimeNano   creation time of the message
   * @param offset           offset of the message
   * @return instance of {@link IntermediateMessageTuple}
   */
  public static IntermediateMessageTuple fromData(Data message, boolean delete, EntityName streamName, Data key, long createTimeNano, Offset offset){
    return new IntermediateMessageTuple(message, delete, key, streamName, createTimeNano, offset);
  }
}
