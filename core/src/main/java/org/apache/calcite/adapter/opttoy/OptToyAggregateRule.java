package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;

import java.util.function.Predicate;

public class OptToyAggregateRule extends ConverterRule {
  OptToyAggregateRule() {
    super(
        LogicalAggregate.class,
        (Predicate<RelNode>) r -> true,
        Convention.NONE,
        OptToyConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "OptToyAggregateRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalAggregate agg = (LogicalAggregate) rel;
    final RelTraitSet traitSet = agg.getTraitSet().replace(OptToyConvention.INSTANCE);
    try {
      return new OptToyAggregate(
          rel.getCluster(),
          traitSet,
          convert(agg.getInput(), traitSet.simplify()),
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    } catch (InvalidRelException e) {
      return null;
    }
  }
}
