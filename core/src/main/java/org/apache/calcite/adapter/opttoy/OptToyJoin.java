package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

public class OptToyJoin extends Join implements OptToyRel {
  OptToyJoin(RelOptCluster cluster, RelTraitSet traits,
      RelNode left, RelNode right, RexNode condition,
      Set<CorrelationId> variablesSet, JoinRelType joinType) {

    super(cluster, traits, left, right, condition, variablesSet, joinType);
    assert getConvention() instanceof OptToyConvention;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    mq.getPrivacy(this, null);
    return super.computeSelfCost(planner, mq).multiplyBy(.0001);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new OptToyJoin(getCluster(), traitSet, left, right,
        conditionExpr, variablesSet, joinType);
  }
}
