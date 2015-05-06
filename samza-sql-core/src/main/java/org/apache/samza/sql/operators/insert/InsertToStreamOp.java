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
package org.apache.samza.sql.operators.insert;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;

import java.util.StringTokenizer;

public class InsertToStreamOp extends SimpleOperator implements TupleOperator {
  private InsertToStreamSpec spec;

  private SystemStream outputStream;
  /**
   * Ctor of <code>SimpleOperator</code> class
   *
   * @param spec The specification of this operator
   */
  public InsertToStreamOp(InsertToStreamSpec spec) {
    super(spec);
    this.spec = spec;
    assert spec.getOutputName().isStream();
    StringTokenizer tokenizer = new StringTokenizer(spec.getOutputName().getName(), ":");
    assert tokenizer.countTokens() == 2;
    outputStream = new SystemStream(tokenizer.nextToken(), tokenizer.nextToken());
  }

  @Override
  public void process(Tuple tuple, SqlMessageCollector collector) throws Exception {
    // TODO: Separate out insert into and insert stream operators
    if(!tuple.isDelete()) {
      collector.send(new OutgoingMessageEnvelope(outputStream, tuple.getMessage()));
    }
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }
}
