package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.function.Predicate;

public class OptToySortRule extends ConverterRule {
  OptToySortRule() {
    super(
        LogicalSort.class,
        (Predicate<RelNode>) r -> true,
        Convention.NONE,
        OptToyConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "OptToySortRUle");
  }

  public RelNode convert(RelNode rel) {
    final LogicalSort sort = (LogicalSort) rel;
    return new OptToySort(
        sort.getCluster(),
        sort.getTraitSet().replace(OptToyConvention.INSTANCE),
        convert(sort.getInput(), sort.getInput().getTraitSet().replace(OptToyConvention.INSTANCE)),
        sort.getCollation());
  }
}
