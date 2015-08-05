/**
 * Copyright (C) 2015 Trustees of Indiana University
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.calcite.aggregates;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
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
import org.apache.samza.sql.operators.aggregate.api.StreamingAggregate;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.*;

public class StreamingAggregateImpl implements StreamingAggregate {
  private final List<Grouping> groups = Lists.newArrayList();
  private final ImmutableBitSet unionGroups;
  private final int outputRowLength;
  private final ImmutableList<AccumulatorFactory> accumulatorFactories;
  private Schema inputType;


  public StreamingAggregateImpl(Aggregate rel) {
    ImmutableBitSet union = ImmutableBitSet.of();

    List<RelCollation> collations = rel.getInput().getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);

    if (rel.getGroupSets() != null) {
      for (ImmutableBitSet group : rel.getGroupSets()) {
        union = union.union(group);
        groups.add(new GroupingImpl2(group, collations, inputType));
      }
    }

    this.unionGroups = union;
    this.outputRowLength = unionGroups.cardinality()
        + (rel.indicator ? unionGroups.cardinality() : 0)
        + rel.getAggCallList().size();

    ImmutableList.Builder<AccumulatorFactory> builder = ImmutableList.builder();
    for (AggregateCall aggregateCall : rel.getAggCallList()) {
      builder.add(getAccumulator(aggregateCall));
    }
    accumulatorFactories = builder.build();
  }

  protected AccumulatorFactory getAccumulator(final AggregateCall aggregateCall) {
    if (aggregateCall.getAggregation() == SqlStdOperatorTable.COUNT) {
      return new AccumulatorFactory() {
        @Override
        public Accumulator get() {
          return new CountAccumulator(aggregateCall, inputType);
        }
      };
    }

    return null;
  }

  @Override
  public void init(TaskContext context, Schema inputType) {
    this.inputType = inputType;
  }

  @Override
  public void send(Tuple tuple, MessageCollector messageCollector) {
    if (inputType == null) {
      // TODO: proper error handling
      throw new RuntimeException("Unknow input type.");
    }

  }

  private class GroupingImpl2 implements Grouping {
    private final Row EMPTY_ROW = new Row(new Object[]{});
    private final Schema inputType;
    private final ImmutableBitSet grouping;
    // TODO: Fix the eviction policy based on time outs.
    private final EvictingQueue<HigherOrderAggregation> evictingAccumulators = EvictingQueue.create(10);
    private final List<RelCollation> collationsOfInput;
    private final ImmutableBitSet orderedIncreasingFields;

    public GroupingImpl2(ImmutableBitSet grouping, List<RelCollation> collationsOfInput, Schema inputType) {
      this.grouping = grouping;
      this.collationsOfInput = collationsOfInput;
      this.inputType = inputType;

      if (collationsOfInput != null && collationsOfInput.size() > 0) {
        this.orderedIncreasingFields = extractOrderedFields();
      } else {
        this.orderedIncreasingFields = ImmutableBitSet.of();
      }
    }

    private ImmutableBitSet extractOrderedFields() {
      List<Integer> orderedAndIncreasningFields = new ArrayList<Integer>();
      for (RelCollation collation : collationsOfInput) {
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
          if (fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING ||
              fieldCollation.getDirection() == RelFieldCollation.Direction.STRICTLY_ASCENDING) {
            orderedAndIncreasningFields.add(fieldCollation.getFieldIndex());
          }
        }
      }

      return ImmutableBitSet.of(orderedAndIncreasningFields);
    }

    @Override
    public void send(Tuple tuple, MessageCollector messageCollector) {
      if (!(tuple instanceof IntermediateMessageTuple)) {
        throw new IllegalArgumentException(
            "Requires a instance of IntermediateMessageTuple but found " + tuple.getClass());
      }

      RowBuilder orderColumnRowBuilder;
      Row orderedKey;

      IntermediateMessageTuple intermediateMessageTuple = (IntermediateMessageTuple) tuple;

      Object[] values = DataUtils.dataToObjectArray(intermediateMessageTuple.getMessage(), inputType);

      RowBuilder rowBuilder = Row.newBuilder(grouping.cardinality());

      for (Integer i : grouping) {
        rowBuilder.set(i, values[i]);
      }

      Row key = rowBuilder.build();

      // TODO: This assumes that group by includes the sorted fields. We need to handle other case.
      if (!orderedIncreasingFields.isEmpty()) {
        orderColumnRowBuilder = Row.newBuilder(orderedIncreasingFields.cardinality());

        for (Integer i : orderedIncreasingFields) {
          orderColumnRowBuilder.set(i, values[i]);
        }

        orderedKey = orderColumnRowBuilder.build();
      } else {
        orderedKey = EMPTY_ROW;
      }

      if (!evictingAccumulators.contains(orderedKey)) {
        // TODO: throw partials for old aggregations

        HigherOrderAggregation hoAgg = new HigherOrderAggregation(orderedKey);
        AccumulatorList accumulatorList = new AccumulatorList();
        for (AccumulatorFactory factory : accumulatorFactories) {
          accumulatorList.add(factory.get());
        }
        hoAgg.put(key, accumulatorList);
        hoAgg.get(key).send(tuple);

        evictingAccumulators.add(hoAgg);
      } else {
        for(HigherOrderAggregation higherOrderAggregation : evictingAccumulators){
          if(higherOrderAggregation.aggregateGrouping.equals(orderedKey)){
            if(!higherOrderAggregation.contains(key)){
              AccumulatorList accumulatorList = new AccumulatorList();
              for (AccumulatorFactory factory : accumulatorFactories) {
                accumulatorList.add(factory.get());
              }

              higherOrderAggregation.put(key, accumulatorList);
            }

            higherOrderAggregation.get(key).send(tuple);
          }
        }
      }
    }

    class HigherOrderAggregation {
      private final Row aggregateGrouping;
      private final Map<Row, AccumulatorList> accumulators;

      public HigherOrderAggregation(Row r) {
        aggregateGrouping = r;
        accumulators = new HashMap<Row, AccumulatorList>();
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof HigherOrderAggregation)) {
          return false;
        }

        return aggregateGrouping.equals(((HigherOrderAggregation) obj).aggregateGrouping);
      }

      public void put(Row key, AccumulatorList value) {
        accumulators.put(key, value);
      }

      public AccumulatorList get(Row key) {
        return accumulators.get(key);
      }

      public boolean contains(Row key) {
        return accumulators.containsKey(key);
      }
    }
  }

  private static class CountAccumulator implements Accumulator {
    // TODO: We need to persist (or some thing similar) the accumulators to handle late arrivals.
    private final AggregateCall call;
    private final Schema inputType;
    long cnt;

    public CountAccumulator(AggregateCall call, Schema inputType) {
      this.call = call;
      this.inputType = inputType;
      cnt = 0;
    }

    @Override
    public void send(Tuple tuple) {
      boolean notNull = true;
      Object[] inputRow = DataUtils.dataToObjectArray(tuple.getMessage(), inputType);
      for (Integer i : call.getArgList()) {
        // TODO: Does this handle COUNT(*)?
        if (inputRow.length <= i || inputRow[i] == null) {
          notNull = false;
          break;
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

    @Override
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
