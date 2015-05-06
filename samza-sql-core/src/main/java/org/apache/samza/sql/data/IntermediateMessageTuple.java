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

/**
 * Defines a tuple passed between operators in a query plan.
 */
public class IntermediateMessageTuple implements Tuple {
  private final Object[] processedMessage;
  private final Data message;
  private final boolean delete;
  private final Data key;
  private final EntityName streamName;

  /**
   * Constructor for {@link IntermediateMessageTuple}
   *
   * @param message     actual value of this tuple
   * @param delete      whether this tuple indicates a deletion from time-varying relation
   * @param key         unique key of the tuple
   * @param streamName  stream name corresponding to time-varying relation this tuple belongs to
   */
  public IntermediateMessageTuple(Data message, boolean delete, Data key, EntityName streamName) {
    this.message = message;
    this.delete = delete;
    this.key = key;
    this.streamName = streamName;
    this.processedMessage = null;
  }

  /**
   * Constructor for {@link IntermediateMessageTuple} which carries a processed message. Process messaged is an array
   * of objects and is a result of operation like project which changes the schema of the output message.
   *
   * @param processedMessage value of this tuple.
   * @param delete           whether this tuple indicates a deletion from time-varying relation
   * @param streamName       stream name corresponding to time-varying relation this tuple belongs to
   * @param key              unique key of the tuple
   */
  public IntermediateMessageTuple(Object[] processedMessage, boolean delete, EntityName streamName, Data key) {
    this.processedMessage = processedMessage;
    this.delete = delete;
    this.streamName = streamName;
    this.key = key;
    this.message = null;
  }

  public Object[] getProcessedMessage() {
    return processedMessage;
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
  public EntityName getStreamName() {
    return streamName;
  }

  public boolean isProcessedMessage(){
    return message == null && processedMessage != null && processedMessage.length > 0;
  }

  /**
   * Creates instance of {@link IntermediateMessageTuple} from a tuple.
   * @param t           original tuple
   * @param delete      is this tuple indicates a deletion from time-varying relation
   * @param streamName  stream name corresponding to time-varying relation this tuple belongs to
   * @return instance of {@link IntermediateMessageTuple}
   */
  public static IntermediateMessageTuple fromTuple(Tuple t, boolean delete, EntityName streamName){
    return new IntermediateMessageTuple(t.getMessage(), delete, t.getKey(), streamName);
  }

  /**
   * Creates instance of {@link IntermediateMessageTuple} from a array of field values.
   *
   * @param processedMessage array of field values
   * @param delete           is this tuple indicates a deletion from time-varying relation
   * @param streamName       stream name corresponding to time-varying relation this tuple belongs to
   * @param key              unique key of the tuple
   * @return instance of {@link IntermediateMessageTuple}
   */
  public static IntermediateMessageTuple fromProcessedMessage(Object[] processedMessage, boolean delete, EntityName streamName, Data key){
    return new IntermediateMessageTuple(processedMessage, delete, streamName, key);
  }
}
