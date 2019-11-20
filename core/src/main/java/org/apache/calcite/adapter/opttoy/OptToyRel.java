package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.rel.RelNode;

public interface OptToyRel extends RelNode {
  enum PrivacyModeDef {
    SECURE,
    NOTSECURE
  }

  enum Partitioning {
    DISTRIBUTED,
    LOCAL
  }
}
