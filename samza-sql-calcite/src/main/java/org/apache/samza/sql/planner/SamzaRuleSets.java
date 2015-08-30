package org.apache.samza.sql.planner;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.RuleSet;

import java.util.Iterator;

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
          FilterJoinRule.JOIN,
          AbstractConverter.ExpandConversionRule.INSTANCE,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          SemiJoinRule.INSTANCE,
          AggregateRemoveRule.INSTANCE,
          UnionToDistinctRule.INSTANCE,
          ProjectRemoveRule.INSTANCE,
          AggregateJoinTransposeRule.INSTANCE,
          CalcRemoveRule.INSTANCE,
          SortRemoveRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE).build();

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

  public static RuleSet[] getBasicRules() {
    /*
     * Calcite planner takes an array of RuleSet and we can refer to them by index to activate
     * each rule set for transforming the query plan based on different criteria.
     */
    final ImmutableSet<RelOptRule> streamRuleSet = ImmutableSet.<RelOptRule>builder()
        .addAll(StreamRules.RULES)
        .build();

    return new RuleSet[]{new SamzaRuleSet(defaultRules), new SamzaRuleSet(streamRuleSet),
        new SamzaRuleSet(abstractRelRules)};
  }

  private static class SamzaRuleSet implements RuleSet {
    final ImmutableSet<RelOptRule> rules;

    public SamzaRuleSet(ImmutableSet<RelOptRule> rules) {
      this.rules = rules;
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return null;
    }
  }
}
