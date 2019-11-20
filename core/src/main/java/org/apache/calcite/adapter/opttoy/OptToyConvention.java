package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

public enum OptToyConvention implements Convention {
  INSTANCE;

  public static final double COST_MULTIPLIER = 2.0d;

  @Override
  public Class getInterface() {
    return OptToyRel.class;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public String getName() {
    return "OPTTOY";
  }

  @Override
  public boolean canConvertConvention(Convention toConvention) {
    return false;
  }

  @Override
  public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
    return false;
  }

  @Override
  public RelTraitDef getTraitDef() {
    return ConventionTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return this == trait;
  }

  @Override
  public void register(RelOptPlanner planner) {}
}
