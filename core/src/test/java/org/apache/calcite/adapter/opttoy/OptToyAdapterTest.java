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
package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.opttoy.OptToyConvention;
import org.apache.calcite.adapter.opttoy.OptToyConverterRule;
import org.apache.calcite.adapter.opttoy.OptToyRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlannerTraitTest;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.Validate;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Will start embedded cassandra cluster and populate it from local {@code twissandra.cql} file.
 * All configuration files are located in test classpath.
 *
 * <p>Note that tests will be skipped if running on JDK11+ (which is not yet supported by cassandra)
 * see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 */
// force tests to run sequentially (maven surefire and failsafe are running them in parallel)
// seems like some of our code is sharing static variables (like Hooks) which causes tests
// to fail non-deterministically (flaky tests).
public class OptToyAdapterTest {

  private static Planner planner;
  private static VolcanoPlanner optimizer;
  private static FrameworkConfig calciteConfig;
  private static SchemaPlus pdnSchema;
  private static CalciteConnection calciteConnection;
  private static JdbcConvention convention;

  @BeforeClass
  public static void setUp() throws ClassNotFoundException, SQLException {
    // run tests only if explicitly enabled
    String url = "jdbc:postgresql://localhost:5432/tpch_unioned";
    Properties props = new Properties();
    props.setProperty("caseSensitive", "false");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
    calciteConnection = connection.unwrap(CalciteConnection.class);
    Class.forName("org.postgresql.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(url);
    dataSource.setUsername("madhav");
    dataSource.setPassword("password");
    JdbcSchema schema =
        JdbcSchema.create(calciteConnection.getRootSchema(), "name", dataSource, null, null);
    for (String tableName : schema.getTableNames()) {
      Table table = schema.getTable(tableName);
      calciteConnection.getRootSchema().add(tableName, table);
    }
    pdnSchema = calciteConnection.getRootSchema();
    SqlParser.Config parserConf =
        SqlParser.configBuilder().setCaseSensitive(false).setLex(Lex.MYSQL).build();
    calciteConfig =
        Frameworks.newConfigBuilder()
            .defaultSchema(pdnSchema)
            .parserConfig(parserConf)
            .programs(Programs.ofRules(FilterProjectTransposeRule.INSTANCE))
            .build();

    final Expression expression =
        calciteConnection.getRootSchema() != null
            ? Schemas.subSchemaExpression(
                calciteConnection.getRootSchema(), "name", JdbcCatalogSchema.class)
            : Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    final SqlDialect dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
    convention = JdbcConvention.of(dialect, expression, "name");

    planner = Frameworks.getPlanner(calciteConfig);

    // SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl((RelDataTypeSystem.DEFAULT);

    final RelDataTypeFactory typeFactory = planner.getTypeFactory();
  }

  public static SqlToRelConverter createSqlToRelConverter() {
    RelDataTypeFactory typeFactory = planner.getTypeFactory();

    final Prepare.CatalogReader catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(pdnSchema),
            CalciteSchema.from(pdnSchema).path(null),
            (JavaTypeFactory) planner.getTypeFactory(),
            calciteConnection.config());

    final Context context = calciteConfig.getContext();
    SqlConformance conformance = SqlConformanceEnum.DEFAULT;
    if (context != null) {
      final CalciteConnectionConfig connectionConfig =
          context.unwrap(CalciteConnectionConfig.class);
      if (connectionConfig != null) {
        conformance = connectionConfig.conformance();
      }
    }

    final SqlValidator validator =
        new LocalValidatorImpl(
            calciteConfig.getOperatorTable(), catalogReader, typeFactory, conformance);
    validator.setIdentifierExpansion(true);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    RelOptCluster cluster = RelOptCluster.create(optimizer, rexBuilder);

    final SqlToRelConverter.ConfigBuilder configBuilder =
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true).withDecorrelationEnabled(true);
    SqlToRelConverter.Config config = configBuilder.build();
    return new SqlToRelConverter(
        null, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
  }

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }
  /**
   * Creates a config builder that will contain a view, "MYVIEW", and also the SCOTT JDBC schema,
   * whose tables implement {@link org.apache.calcite.schema.TranslatableTable}.
   */
  static Frameworks.ConfigBuilder expandingConfig(Connection connection) throws SQLException {
    final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    final SchemaPlus root = calciteConnection.getRootSchema();
    CalciteAssert.SchemaSpec spec = CalciteAssert.SchemaSpec.SCOTT;
    CalciteAssert.addSchema(root, spec);
    final String viewSql =
        String.format(Locale.ROOT, "select * from \"%s\".\"%s\" where 1=1", spec.schemaName, "EMP");

    // create view
    ViewTableMacro macro =
        ViewTable.viewMacro(
            root, viewSql, Collections.singletonList("test"), Arrays.asList("test", "view"), false);

    // register view (in root schema)
    root.add("MYVIEW", macro);

    return Frameworks.newConfigBuilder().defaultSchema(root);
  }

  @Test
  public void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final RelNode root = RelBuilder.create(config().build()).scan("EMP").build();
    assertThat(root, hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  static RelOptCluster newCluster(VolcanoPlanner planner) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  @Test
  public void testTPCHQuery10() throws ValidationException, SqlParseException {
    String sql =
        "select\n"
            + "  c.c_custkey,\n"
            + "  c.c_name,\n"
            + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
            + "  c.c_acctbal,\n"
            + "  n.n_name,\n"
            + "  c.c_address,\n"
            + "  c.c_phone,\n"
            + "  c.c_comment\n"
            + "from\n"
            + "  customer c,\n"
            + "  orders o,\n"
            + "  lineitem l,\n"
            + "  nation n\n"
            + "where\n"
            + "  c.c_custkey = o.o_custkey\n"
            + "  and l.l_orderkey = o.o_orderkey\n"
            + "  and o.o_orderdate >= date '1994-03-01'\n"
            + "  and o.o_orderdate < date '1994-03-01' + interval '3' month\n"
            + "  and l.l_returnflag = 'R'\n"
            + "  and c.c_nationkey = n.n_nationkey\n"
            + "group by\n"
            + "  c.c_custkey,\n"
            + "  c.c_name,\n"
            + "  c.c_acctbal,\n"
            + "  c.c_phone,\n"
            + "  n.n_name,\n"
            + "  c.c_address,\n"
            + "  c.c_comment\n"
            + "order by\n"
            + "  revenue desc\n"
            + "limit 20";
    testTPCHQuery(sql);
  }

  @Test
  public void testTPCHQuery5() throws ValidationException, SqlParseException {
    String sql =
        "select\n"
            + "  n.n_name,\n"
            + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue\n"
            + "\n"
            + "from\n"
            + "  customer c,\n"
            + "  orders o,\n"
            + "  lineitem l,\n"
            + "  supplier s,\n"
            + "  nation n,\n"
            + "  region r\n"
            + "\n"
            + "where\n"
            + "  c.c_custkey = o.o_custkey\n"
            + "  and l.l_orderkey = o.o_orderkey\n"
            + "  and l.l_suppkey = s.s_suppkey\n"
            + "  and c.c_nationkey = s.s_nationkey\n"
            + "  and s.s_nationkey = n.n_nationkey\n"
            + "  and n.n_regionkey = r.r_regionkey\n"
            + "  and r.r_name = 'EUROPE'\n"
            + "--  and o.o_orderdate >= date '1997-01-01'\n"
            + "--  and o.o_orderdate < date '1997-01-01' + interval '1' year\n"
            + "group by\n"
            + "  n.n_name\n"
            + "\n"
            + "order by\n"
            + "  revenue desc";

    testTPCHQuery(sql);
  }

  @Test
  public void testSimpleQuery() throws ValidationException, SqlParseException {
    String sql = "select c_mktsegment from customer where c_mktsegment = 'HOUSEHOLD'";
    testTPCHQuery(sql);
  }

  @Test
  public void testTPCHQuery3() throws ValidationException, SqlParseException {
    String sql =
        "select\n"
            + "  l.l_orderkey,\n"
            + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "\n"
            + "from\n"
            + "  customer c,\n"
            + "  orders o,\n"
            + "  lineitem l\n"
            + "\n"
            + "where\n"
            + "  c.c_mktsegment = 'HOUSEHOLD'\n"
            + "  and c.c_custkey = o.o_custkey\n"
            + "  and l.l_orderkey = o.o_orderkey\n"
            + "--  and o.o_orderdate < date '1995-03-25'\n"
            + "--  and l.l_shipdate > date '1995-03-25'\n"
            + "\n"
            + "group by\n"
            + "  l.l_orderkey,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "order by\n"
            + "  revenue desc,\n"
            + "  o.o_orderdate\n"
            + "limit 10";
    testTPCHQuery(sql);
  }

  public void testTPCHQuery(String sql) throws SqlParseException, ValidationException {

    optimizer = new VolcanoPlanner();
    optimizer.addRelTraitDef(ConventionTraitDef.INSTANCE);
    optimizer.addRule(new OptToyRules.OptToyTestFilter());
    // add rules
    optimizer.addRule(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
    optimizer.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
    optimizer.addRule(PruneEmptyRules.PROJECT_INSTANCE);
    optimizer.addRule(new OptToyFilterRule());
    optimizer.addRule(new OptToyProjectRule());
    optimizer.addRule(new OptToySortRule());
    optimizer.addRule(new OptToyAggregateRule());
    optimizer.addRule(new OptToyJoinRule());
    //optimizer.addRule(new OptToyConverterRule());

    // add ConverterRule

    for (RelOptRule rule : EnumerableRules.rules()) {
      optimizer.addRule(rule);
    }
    /*
    optimizer.addRule(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
       */
    SqlNode node = planner.parse(sql);
    node = planner.validate(node);
    SqlToRelConverter converter = createSqlToRelConverter();
    RelRoot n = converter.convertQuery(node, true, true);
    RelNode relNode = n.rel;

    // TODO(madhavsuresh): only works with needsValidation set to true.
    RelTraitSet desiredTraits =
        relNode.getCluster().traitSet().replace(OptToyConvention.INSTANCE);
    desiredTraits.replace(EnumerableConvention.INSTANCE);
    relNode = optimizer.changeTraits(relNode, desiredTraits);
    optimizer.setRoot(relNode);

    RelNode n2 = optimizer.findBestExp();
    System.out.println(RelOptUtil.toString(n2));
  }

  @Test
  public void testOptToyFilter() {

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    // Below two lines are important for the planner to use collation trait and generate merge join
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.registerAbstractRelationalRules();

    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);

    RelOptCluster cluster = newCluster(planner);

    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan =
        relBuilder
            .values(new String[] {"id", "name"}, "2", "a", "1", "b")
            .values(new String[] {"id", "name"}, "1", "x", "2", "y")
            .join(JoinRelType.INNER, "id")
            .build();

    RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode newRoot = planner.changeTraits(logicalPlan, desiredTraits);
    OptToyRules.OptToyTestFilter oF = new OptToyRules.OptToyTestFilter();
    planner.addRule(oF);
    planner.setRoot(newRoot);

    /*
    RelNode bestExp = planner.findBestExp();
    VolcanoPlanner p = new VolcanoPlanner();
    p.addRelTraitDef(ConventionTraitDef.INSTANCE);
    p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    p.registerAbstractRelationalRules();
    RelOptCluster cluster = newCluster(p);
    final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan = relBuilder
            .values(new String[]{"id", "name"}, "2", "a", "1", "b")
            .values(new String[]{"id", "name"}, "1", "x", "2", "y")
            .join(JoinRelType.INNER, "id")
            .build();
    RelTraitSet desiredTraits =
            cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode newRoot = p.changeTraits(logicalPlan, desiredTraits);
    p.setRoot(newRoot);

     */
    RelNode result = planner.findBestExp();
    System.out.println(RelOptUtil.toString(result));
  }

  @Test
  public void testTraitConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan = relBuilder
            .values(new String[]{"id", "name"}, "2", "a", "1", "b")
            .values(new String[]{"id", "name"}, "1", "x", "2", "y")
            .join(JoinRelType.INNER, "id")
            .build();
    RelTraitSet desiredTraits =
            cluster.traitSet().replace(OptToyConvention.INSTANCE);
    final RelNode newRoot = planner.changeTraits(logicalPlan, desiredTraits);
    planner.setRoot(newRoot);
    RelNode bestExp = planner.findBestExp();
  }

  private static class LocalValidatorImpl extends SqlValidatorImpl {
    protected LocalValidatorImpl(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }
  }
}
