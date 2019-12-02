package org.apache.calcite.adapter.opttoy;

import java.util.HashMap;

/**
 * PrivacyProperties provides a strongly typed
 *
 */
public class PrivacyProperties {

  public static PrivacyMode translatePrivacyMode(String grantee) {
    switch (grantee) {
    case "public_attribute":
      return PrivacyMode.PUBLIC;
    /* The grantee field is currently overloaded to capture both
     * the privacy mode as well as the replication scheme. This is a
     * hack to ensure that in RelMdPrivacy when the grantee is being read
     * in, partitioning fields are typed correctly.
     */
    case "replicated":
    case "partitioned_on":
      return PrivacyMode.NONE;
    default:
      return PrivacyMode.PRIVATE;
    }
  }

  public static Partitioning translateParitioning(String grantee) {
    switch (grantee) {
    case "replicated":
      return Partitioning.REPLICATED;
    case "partitioned_on":
      return Partitioning.PARTITIONED_ON;
    default:
      return Partitioning.NONE;
    }
  }

  public enum PrivacyMode {
    PUBLIC,
    PRIVATE,
    NONE
  }

  public enum Partitioning {
    PARTITIONED_ON,
    LOCAL,
    DISTRIBUTED,
    REPLICATED,
    NONE
  }

  private Partitioning operatorPartitioning;
  private PrivacyMode operatorPrivacyMode;
  private HashMap<String, PrivacyMode> columnPrivacyMap;
  private HashMap<String, Partitioning> columnPartitioningMap;

  public PrivacyMode getColumnPrivacyMode(final String column) {
    return this.columnPrivacyMap.get(column);
  }
  public PrivacyMode getOperatorPrivacyMode() {
    return this.operatorPrivacyMode;
  }

  public PrivacyProperties() {
    this.columnPrivacyMap = new HashMap<>();
    this.columnPartitioningMap = new HashMap<>();
  }

  public void setColumnPrivacyMode(final String column, final PrivacyMode privacyMode) {
    // TODO(madhavsuresh): grantees currently are duplicated. This occurs only for
    // public attributes, since by default the columns are private. There is the
    // PostgreSQL grantee information, then the privacy annotations.
    if (!this.columnPrivacyMap.containsKey(column)) {
      this.columnPrivacyMap.put(column, privacyMode);
    }
  }

  public void setOperatorPrivacyMode(final PrivacyMode privacyMode) {
    this.operatorPrivacyMode = privacyMode;
  }

  public Partitioning getColumnPartitioning(final String column) {
    return this.columnPartitioningMap.get(column);
  }

  public void setOperatorPartitioning(final Partitioning partitioning) {
    this.operatorPartitioning = partitioning;
  }

  public void setColumnPartitioning(final String column, final Partitioning partitioning) {
    this.columnPartitioningMap.put(column, partitioning);
  }
}
