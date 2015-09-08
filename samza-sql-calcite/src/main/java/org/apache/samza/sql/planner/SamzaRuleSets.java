package org.apache.samza.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.samza.sql.planner.logical.rules.*;

import java.util.Iterator;
import java.util.List;

public class SamzaRuleSets {

  private static final boolean COMMUTE =
      "true".equals(
          System.getProperties().getProperty("calcite.enable.join.commute"));

  /**
   * Default set of query planner rules use by Samza including rules from
   * VolcanoPlanner#registerAbstractRelationalRules.
   * <p/>
   * TODO: Check whether order of the rules make a difference.
   */
  private static final ImmutableSet<RelOptRule> defaultRules =
      ImmutableSet.<RelOptRule>builder().add(
          TableScanRule.INSTANCE,
          COMMUTE ? JoinAssociateRule.INSTANCE : ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
//          SemiJoinRule.INSTANCE,
//          AggregateRemoveRule.INSTANCE,
//          UnionToDistinctRule.INSTANCE,
//          ProjectRemoveRule.INSTANCE,
//          AggregateJoinTransposeRule.INSTANCE,
//          CalcRemoveRule.INSTANCE,
//          SortRemoveRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE).build();

  private static final ImmutableSet<RelOptRule> defaultRulesAlt =
      ImmutableSet.<RelOptRule>builder().add(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
          COMMUTE
              ? JoinAssociateRule.INSTANCE
              : ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE
      ).build();

  private static final ImmutableSet<RelOptRule> abstractRelRules =
      ImmutableSet.<RelOptRule>builder().add(
          AggregateProjectPullUpConstantsRule.INSTANCE,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          FilterMergeRule.INSTANCE
      ).build();

  private static final List<RelOptRule> ENUMERABLE_RULES =
      ImmutableList.of(
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

  private static final ImmutableSet<RelOptRule> calcitetoSamzaConversionRules =
      ImmutableSet.<RelOptRule>builder().add(
          AbstractConverter.ExpandConversionRule.INSTANCE,
          SamzaScanRule.INSTANCE,
          SamzaFilterRule.INSTANCE,
          SamzaProjectRule.INSTANCE,
          SamzaAggregateRule.INSTANCE,
          SamzaJoinRule.INSTANCE                  // TODO: Window, Modify, Sort, Limit, Union
          ).build();

  public static RuleSet[] getRuleSets() {
    /*
     * Calcite planner takes an array of RuleSet and we can refer to them by index to activate
     * each rule set for transforming the query plan based on different criteria.
     */
    final ImmutableSet<RelOptRule> logicalRules = ImmutableSet.<RelOptRule>builder()
        .addAll(StreamRules.RULES)
        .addAll(calcitetoSamzaConversionRules)
        .build();

    return new RuleSet[]{new SamzaRuleSet(logicalRules)};
  }

  private static class SamzaRuleSet implements RuleSet {
    final ImmutableSet<RelOptRule> rules;

    public SamzaRuleSet(ImmutableSet<RelOptRule> rules) {
      this.rules = rules;
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }
}
