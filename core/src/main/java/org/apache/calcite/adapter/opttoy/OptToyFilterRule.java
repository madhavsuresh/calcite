package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

import java.util.function.Predicate;

public class OptToyFilterRule extends ConverterRule {
  OptToyFilterRule() {
    super(
        LogicalFilter.class,
        (Predicate<LogicalFilter>) RelOptUtil::containsMultisetOrWindowedAgg,
        Convention.NONE,
        OptToyConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "OptToyFilterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalFilter filter = (LogicalFilter) rel;
    return new OptToyFilter(
        rel.getCluster(),
        rel.getTraitSet().replace(OptToyConvention.INSTANCE),
        convert(
            filter.getInput(), filter.getInput().getTraitSet().replace(OptToyConvention.INSTANCE)),
        filter.getCondition());
  }
}
