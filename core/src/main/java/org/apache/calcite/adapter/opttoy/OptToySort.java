package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class OptToySort extends Sort implements OptToyRel {
  OptToySort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation) {
    super(cluster, traitSet, child, collation, null, null);
    assert getConvention() instanceof OptToyConvention;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    mq.getPrivacy(this, null);
    return super.computeSelfCost(planner, mq).multiplyBy(.0001);
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    return new OptToySort(getCluster(), traitSet, input, collation);
  }
}
