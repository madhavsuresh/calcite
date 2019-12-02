/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.metadata;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.adapter.opttoy.OptToyConverterRule;
import org.apache.calcite.adapter.opttoy.OptToyFilter;
import org.apache.calcite.adapter.opttoy.OptToyJoin;
import org.apache.calcite.adapter.opttoy.PrivacyProperties;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

public class RelMdPrivacy implements MetadataHandler<BuiltInMetadata.Privacy> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.PRIVACY.method, new RelMdPrivacy());

  private RelMdPrivacy() {}

  @Override
  public MetadataDef<BuiltInMetadata.Privacy> getDef() {
    return BuiltInMetadata.Privacy.DEF;
  }

  // TODO(madhavsuresh): Why isn't this being cached for each call of JdbcTableScan
  public PrivacyProperties getPrivacy(JdbcTableScan rel, RelMetadataQuery mq, RexNode predicate) {
    PrivacyProperties privacyProperties = new PrivacyProperties();
    final DataSource dataSource = rel.jdbcTable.unwrap(DataSource.class);
    String query = new StringBuilder(256).append(
        "SELECT table_name, column_name, grantee from information_schema.column_privileges WHERE table_name='")
        .append(rel.jdbcTable.jdbcTableName)
        .append("'")
        .toString();
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(query);
    ) {
      while (resultSet.next()) {
        final String grantee = resultSet.getString("grantee");
        final String column_name = resultSet.getString("column_name");
        final PrivacyProperties.PrivacyMode privacyMode = PrivacyProperties.translatePrivacyMode(grantee);
        if (privacyMode != PrivacyProperties.PrivacyMode.NONE) {
          privacyProperties.setColumnPrivacyMode(column_name, privacyMode);
        } else {
          privacyProperties.setColumnPartitioning(column_name, PrivacyProperties.translateParitioning(grantee));
        }
      }

    } catch (SQLException e) {
      System.out.println(e);
    }
    privacyProperties.setOperatorPartitioning(PrivacyProperties.Partitioning.LOCAL);
    privacyProperties.setOperatorPrivacyMode(PrivacyProperties.PrivacyMode.PUBLIC);
    return privacyProperties;
  }

  public PrivacyProperties getPrivacy(OptToyConverterRule.JdbcToOptToyConverter rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getPrivacy(rel.getInput(0), null);
  }

  public PrivacyProperties getPrivacy(Filter filter, RelMetadataQuery mq, RexNode predicate) {
    System.out.println("IN HERE");
    PrivacyProperties inputPrivacy = mq.getPrivacy(filter.getInput(),null);
    RexNode condition = filter.getCondition();
    if (condition.isA(SqlKind.EQUALS)) {
      RexCall x = (RexCall) condition;
      for ( RexNode z : x.operands) {
        if (z.getKind() == SqlKind.LITERAL) {
          RexLiteral l = (RexLiteral)z;
        } else if (z.getKind() == SqlKind.INPUT_REF) {
          RexInputRef l = (RexInputRef)z;
          String columnName = filter.getRowType().getFieldList().get(l.getIndex()).getName();
          //TODO(madhavsuresh): this needs to be an off switch, once any predicate is private, the whole
          // operator should remain private.
          inputPrivacy.setOperatorPrivacyMode(inputPrivacy.getColumnPrivacyMode(columnName));
        }
      }
    }
    return inputPrivacy;
  }

  public PrivacyProperties getPrivacy(Join rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getPrivacy(rel.getLeft(), null);
    //PrivacyProperties p = new PrivacyProperties();
    //p.setOperatorPrivacyMode(PrivacyProperties.PrivacyMode.PUBLIC);
    //return p;
    //mq.getPrivacy(rel.getRight(), null);
    //return null;
  }
  public PrivacyProperties getPrivacy(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getPrivacy(Util.first(rel.getBest(), rel.getOriginal()),null);
  }

  public PrivacyProperties getPrivacy(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    if (rel.getInputs().size() > 0) {
      return mq.getPrivacy(rel.getInput(0), null);
    }
    PrivacyProperties p = new PrivacyProperties();
    p.setOperatorPrivacyMode(PrivacyProperties.PrivacyMode.PRIVATE);
    return p;
  }
}
