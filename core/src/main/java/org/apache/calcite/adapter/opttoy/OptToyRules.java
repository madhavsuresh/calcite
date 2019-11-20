package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class OptToyRules {

  // public static final RelOptRule[] RULES = {OptToyTestFilter.INSTANCE};

  public static class OptToyTestFilter extends RelOptRule {
    public OptToyTestFilter() {
      super(operand(LogicalFilter.class, any()), "OptToyTestFilter");
    }

    public boolean matches(RelOptRuleCall call) {
      call.rel(0);
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();
      if (condition.getKind() == SqlKind.EQUALS) {
        return true;
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      System.out.println("HELL0 WORLD");
    }
  }
}
