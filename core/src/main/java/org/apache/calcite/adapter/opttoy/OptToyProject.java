package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class OptToyProject extends Project implements OptToyRel {

  public OptToyProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() instanceof OptToyConvention;
  }

  public static OptToyProject create(
      final RelNode input, final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster
            .traitSet()
            .replace(OptToyConvention.INSTANCE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project(mq, input, projects));
    return new OptToyProject(cluster, traitSet, input, projects, rowType);
  }

  public OptToyProject copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new OptToyProject(getCluster(), traitSet, input, projects, rowType);
  }

}
