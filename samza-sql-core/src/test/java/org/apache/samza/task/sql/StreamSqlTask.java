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

package org.apache.samza.task.sql;

import java.util.ArrayList;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.OperatorSource;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.factory.TopologyBuilder;
import org.apache.samza.sql.operators.join.StreamStreamJoinSpec;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.window.FullStateTimeWindowOp;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a SQL join operation that joins two streams together using the folowing operations:
 * <ul>
 * <li>a. the two streams are each processed by a window operator to convert to relations
 * <li>b. a join operator is applied on the two relations to generate join results
 * <li>c. an istream operator is applied on join output and convert the relation into a stream
 * <li>d. a partition operator that re-partitions the output stream from istream and send the stream to system output
 * </ul>
 *
 * This example also uses an implementation of <code>SqlMessageCollector</code> (@see <code>OperatorMessageCollector</code>)
 * that uses <code>OperatorRouter</code> to automatically execute the whole paths that connects operators together.
 */
public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {

  private OperatorRouter router;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    this.router.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    this.router.refresh(System.nanoTime(), collector, coordinator);
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // Now, create the operator and connect them via TopologyBuilder
    OperatorSource joinSource1 =
        TopologyBuilder.create().operator(new FullStateTimeWindowOp("fixedWnd2", 10, "kafka:inputStream2", null))
            .stream();
    this.router =
        TopologyBuilder.create().operator(new FullStateTimeWindowOp("fixedWnd1", 10, "kafka:inputStream1", null))
            .operator(new StreamStreamJoinSpec("joinOp", new ArrayList<EntityName>(), null, new ArrayList<String>() {
              {
                add("key1");
                add("key2");
              }
            })).bind(joinSource1)
            .operator(new PartitionSpec("parOp1", null, new SystemStream("kafka", "parOutputStrm1"), "joinKey", 50))
            .build();

    this.router.init(config, context);

  }
}
