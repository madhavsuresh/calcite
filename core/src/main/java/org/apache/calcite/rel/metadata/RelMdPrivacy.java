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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RelMdPrivacy implements MetadataHandler<BuiltInMetadata.Privacy> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.PRIVACY.method, new RelMdPrivacy());

  private RelMdPrivacy() {}

  @Override
  public MetadataDef<BuiltInMetadata.Privacy> getDef() {
    return BuiltInMetadata.Privacy.DEF;
  }

  public Integer getPrivacy(JdbcTableScan rel, RelMetadataQuery mq, RexNode predicate) {
    final DataSource dataSource = rel.jdbcTable.unwrap(DataSource.class);
    String query = new StringBuilder(256).append(
        "SELECT table_name, column_name, grantee from information_schema.column_privileges WHERE table_name='")
        .append(rel.jdbcTable.jdbcTableName)
        .append("'")
        .toString();
    System.out.println(query);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(query);
    ) {
      while(resultSet.next()) {
        //System.out.println(resultSet.getString("grantee"));
      }

    } catch (SQLException e) {
      System.out.println(e);
    }
    return 2;
  }

  public Integer getPrivacy(OptToyConverterRule.JdbcToOptToyConverter rel, RelMetadataQuery mq, RexNode predicate) {
    System.out.println(rel.getInput().getRelTypeName());
    return mq.getPrivacy(rel.getInput(0),null) +1;
  }

  public Integer getPrivacy(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
    for (RelNode node : rel.getRels()) {
      System.out.println(node.getRelTypeName());
      return mq.getPrivacy(node, null) +1;
    }
    return -1;
  }

  public Integer getPrivacy(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    if (rel.getInputs().size()  > 0){
      int k = mq.getPrivacy(rel.getInput(0), null);
      System.out.println(rel.getRelTypeName());
      System.out.println(rel.getInput(0).getRelTypeName());
      System.out.println("RelNode WOW " + k);
      return k+1;
    } else {
      System.out.println("PROJECT? " + rel.getRelTypeName());
    }
    return 0;
    //int k = 0;
  }
}
