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
package org.apache.samza.sql.calcite.aggregates;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.DataUtils;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.aggregate.AccumulatorList;
import org.apache.samza.sql.operators.aggregate.api.Accumulator;
import org.apache.samza.sql.operators.aggregate.api.AccumulatorFactory;
import org.apache.samza.sql.operators.aggregate.api.Grouping;
import org.apache.samza.task.MessageCollector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupingImpl implements Grouping {

  private final ImmutableList<AccumulatorFactory> accumulatorFactories;
  private final ImmutableBitSet grouping;
  private final ImmutableBitSet unionGroups;
  private final Map<Row, AccumulatorList> accumulators = Maps.newHashMap();
  //private final Schema inputType;

  public GroupingImpl(ImmutableBitSet grouping, ImmutableBitSet unionGroups, List<AggregateCall> aggregateCalls, Schema inputType) {
    ImmutableList.Builder<AccumulatorFactory> builder = ImmutableList.builder();
    for (AggregateCall aggregateCall : aggregateCalls) {
      builder.add(getAccumulator(aggregateCall));
    }
    accumulatorFactories = builder.build();
    this.grouping = grouping;
    this.unionGroups = unionGroups;
  }

  @Override
  public void send(Tuple tuple, MessageCollector messageCollector) {

    if (!(tuple instanceof IntermediateMessageTuple)) {
      throw new IllegalArgumentException(
          "Requires a instance of IntermediateMessageTuple but found " + tuple.getClass());
    }

    IntermediateMessageTuple intermediateMessageTuple = (IntermediateMessageTuple) tuple;

    Object[] values = DataUtils.dataToObjectArray(intermediateMessageTuple.getMessage(), null);

    RowBuilder rowBuilder = Row.newBuilder(grouping.cardinality());

    for (Integer i : grouping) {
      rowBuilder.set(i, values[i]);
    }

    Row key = rowBuilder.build();

    if(!accumulators.containsKey(key)){
      AccumulatorList accumulatorList = new AccumulatorList();
      for(AccumulatorFactory factory : accumulatorFactories){
        accumulatorList.add(factory.get());
      }
      accumulators.put(key, accumulatorList);
    }

    accumulators.get(key).send(tuple);
  }

  private AccumulatorFactory getAccumulator(final AggregateCall call) {
    if (call.getAggregation() == SqlStdOperatorTable.COUNT) {
      return new AccumulatorFactory() {
        @Override
        public Accumulator get() {
          return new CountAccumulator(call);
        }
      };
    }

    throw new UnsupportedOperationException("Only COUNT aggregator is supported at this stage.");
  }

  /**
   * Accumulator for calls to the COUNT function.
   */
  private static class CountAccumulator implements Accumulator {
    private final AggregateCall call;
    long cnt;

    public CountAccumulator(AggregateCall call) {
      this.call = call;
      cnt = 0;
    }

    @Override
    public void send(Tuple tuple) {
      boolean notNull = true;
      for (Integer i : call.getArgList()) {
        if (tuple instanceof IntermediateMessageTuple) {
          IntermediateMessageTuple intTuple = (IntermediateMessageTuple) tuple;

//          if (!intTuple.isProcessedMessage()) {
//            throw new IllegalArgumentException("Tuple should contain a processed message.");
//          }
//
//          if (intTuple.getProcessedMessage().length < i) {
//            notNull = false;
//            break;
//          }
//
//          if (intTuple.getProcessedMessage()[i] == null) {
//            notNull = false;
//            break;
//          }
        } else {
          throw new IllegalArgumentException(
              "Requires a instance of IntermediateMessageTuple but found " + tuple.getClass());
        }
      }

      if (notNull) {
        cnt++;
      }
    }

    @Override
    public Object partial() {
      return cnt;
    }

    public Object end() {
      return cnt;
    }
  }

  public static class Row {
    private final Object[] values;

    public Row(Object[] values) {
      this.values = values;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Row
          && Arrays.equals(values, ((Row) obj).values);
    }

    @Override
    public String toString() {
      return Arrays.toString(values);
    }

    public Object getObject(int index) {
      return values[index];
    }

    public int size() {
      return values.length;
    }

    /**
     * Create a RowBuilder object that eases creation of a new row.
     *
     * @param size Number of columns in output data.
     * @return New RowBuilder object.
     */
    public static RowBuilder newBuilder(int size) {
      return new RowBuilder(size);
    }
  }

  /**
   * Utility class to build row objects.
   */
  public static class RowBuilder {
    Object[] values;

    private RowBuilder(int size) {
      values = new Object[size];
    }

    /**
     * Set the value of a particular column.
     *
     * @param index Zero-indexed position of value.
     * @param value Desired column value.
     */
    public void set(int index, Object value) {
      values[index] = value;
    }

    /**
     * Return a Row object *
     */
    public Row build() {
      return new Row(values);
    }

    public int size() {
      return values.length;
    }
  }
}
