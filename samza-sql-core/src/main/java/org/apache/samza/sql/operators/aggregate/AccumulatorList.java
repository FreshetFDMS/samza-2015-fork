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
package org.apache.samza.sql.operators.aggregate;

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.aggregate.api.Accumulator;

import java.util.ArrayList;
import java.util.ListIterator;

public class AccumulatorList extends ArrayList<Accumulator> {
  public void send(Tuple t) {
    for (Accumulator accumulator : this) {
      accumulator.send(t);
    }
  }

  public Object[] partial() {
    Object[] result = new Object[size()];
    ListIterator<Accumulator> itr = listIterator();

    while (itr.hasNext()) {
      result[itr.nextIndex()] = itr.next().partial();
    }

    return result;
  }

  public Object[] end() {
    Object[] result = new Object[size()];
    ListIterator<Accumulator> itr = listIterator();

    while (itr.hasNext()) {
      result[itr.nextIndex()] = itr.next().end();
    }

    return result;
  }
}
