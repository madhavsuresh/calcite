package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class OptToyCost implements RelOptCost {

  // OptToy specific
  public double getPrivacyCost() {
    return 0;
  }

  @Override
  public double getRows() {
    return 0;
  }

  @Override
  public double getCpu() {
    return 0;
  }

  @Override
  public double getIo() {
    return 0;
  }

  @Override
  public boolean isInfinite() {
    return false;
  }

  @Override
  public boolean equals(RelOptCost cost) {
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost cost) {
    return false;
  }

  @Override
  public boolean isLe(RelOptCost cost) {
    return false;
  }

  @Override
  public boolean isLt(RelOptCost cost) {
    return false;
  }

  @Override
  public RelOptCost plus(RelOptCost cost) {
    return null;
  }

  @Override
  public RelOptCost minus(RelOptCost cost) {
    return null;
  }

  @Override
  public RelOptCost multiplyBy(double factor) {
    return null;
  }

  @Override
  public double divideBy(RelOptCost cost) {
    return 0;
  }

  private static class Factory implements RelOptCostFactory {

    public OptToyCost privacyCost() {
      return null;
    }

    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
      return null;
    }

    @Override
    public RelOptCost makeHugeCost() {
      return null;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
      return null;
    }

    @Override
    public RelOptCost makeTinyCost() {
      return null;
    }

    @Override
    public RelOptCost makeZeroCost() {
      return null;
    }
  }
}
