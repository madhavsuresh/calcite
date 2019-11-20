package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.function.Predicate;

public class OptToyProjectRule extends ConverterRule {
  OptToyProjectRule() {
    super(
        LogicalProject.class,
        (Predicate<LogicalProject>) RelOptUtil::containsMultisetOrWindowedAgg,
        Convention.NONE,
        OptToyConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "OptToyProjectRule");
  }

  public RelNode convert(RelNode rel) {
    final LogicalProject project = (LogicalProject) rel;
    return OptToyProject.create(
        convert(
            project.getInput(),
            project.getInput().getTraitSet().replace(OptToyConvention.INSTANCE)),
        project.getProjects(),
        project.getRowType());
  }
}
