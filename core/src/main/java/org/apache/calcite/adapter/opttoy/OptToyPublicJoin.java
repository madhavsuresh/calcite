package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

public class OptToyPublicJoin extends OptToyJoin implements OptToyRel {
  OptToyPublicJoin(final RelOptCluster cluster, final RelTraitSet traits,
      final RelNode left, final RelNode right, final RexNode condition,
      final Set<CorrelationId> variablesSet,
      final JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
    assert getConvention() instanceof  OptToyConvention;
  }


  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    return planner.getCostFactory().makeTinyCost();
  }

  public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new OptToyPublicJoin(getCluster(), traitSet, left, right,
        conditionExpr, variablesSet, joinType);
  }
}
