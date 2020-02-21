package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.rel.RelNode;

public interface OptToyRel extends RelNode {
  /*
  void implement (Implementor impementor);
  enum PrivacyModeDef {
    SECURE,
    NOTSECURE
  }
   */

  enum Partitioning {
    DISTRIBUTED,
    LOCAL
  }

  class Implementor {
  }

}
