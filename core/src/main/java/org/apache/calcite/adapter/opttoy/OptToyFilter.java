package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class OptToyFilter extends Filter implements OptToyRel {
  OptToyFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() instanceof OptToyConvention;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    mq.getPrivacy(this, null);
    return super.computeSelfCost(planner, mq).multiplyBy(.0001);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new OptToyFilter(getCluster(), traitSet, input, condition);
  }

}
