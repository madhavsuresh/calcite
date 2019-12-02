package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class OptToyPublicJoinRule extends ConverterRule {
  OptToyPublicJoinRule() {
    //TODO(madhavsuresh): this does not need to be a converterrule since the input and output traits are the same.
    super(
        OptToyJoin.class,
        (Predicate<OptToyJoin>) r -> r.getPrivacy() == PrivacyProperties.PrivacyMode.PUBLIC,
        OptToyConvention.INSTANCE,
        OptToyConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "OptToyPublicJoinRule");
  }

  public RelNode convert(RelNode rel) {
    OptToyJoin join = (OptToyJoin) rel;
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      if (!(input.getConvention() instanceof OptToyConvention)) {
        input =
            convert(
                input,
                input.getTraitSet()
                    .replace(OptToyConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    final RelOptCluster cluster = join.getCluster();
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final JoinRelType joinType = join.getJoinType();
    final RelTraitSet traitSet =
        cluster.traitSetOf(OptToyConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableNestedLoopJoin(mq, left, right, joinType));
    return new OptToyPublicJoin(join.getCluster(), traitSet, left, right, join.getCondition(), join.getVariablesSet(),
        join.getJoinType());

  }
}
