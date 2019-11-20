package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;
import java.util.function.Predicate;

public class OptToyConverterRule extends ConverterRule {

  // TODO(madhavsuresh): this isn't normally public.
  public OptToyConverterRule(JdbcConvention out, RelBuilderFactory relBuilderFactory) {
    super(
        JdbcTableScan.class,
        (Predicate<RelNode>) r -> true,
        out,
        OptToyConvention.INSTANCE,
        relBuilderFactory,
        "OptToyDummyRule");
    System.out.println("In Opt Toy Dummy Rule");

  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    //mq.getPrivacy(rel, null);
    return new JdbcToOptToyConverter(rel.getCluster(), newTraitSet, rel);
  }

  public static class JdbcToOptToyConverter extends ConverterImpl implements OptToyRel {

    protected JdbcToOptToyConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      // TODO(madhavsuresh): don't actually know what this does
      super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcToOptToyConverter(getCluster(), traitSet, sole(inputs));
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(2);
    }
  }
}
