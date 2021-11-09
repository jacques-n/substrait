package io.substrait.isthmus.metadata;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.base.Suppliers;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

public class LambdaMetadataSupplier implements Supplier<RelMetadataQuery> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LambdaMetadataSupplier.class);

  // lazy initialize a supplier so we don't maintain multiple caches.
  private static final Supplier<Supplier<RelMetadataQuery>> INSTANCE = Suppliers.memoize(() -> new LambdaMetadataSupplier());

  public static Supplier<RelMetadataQuery> instance() {
    return INSTANCE.get();
  }

  private LambdaHandlerCache handlerCache;

  public LambdaMetadataSupplier() {
    this(new LegacyToLambdaGenerator());
  }

  public LambdaMetadataSupplier(LambdaProvider provider) {
    handlerCache = new LambdaHandlerCache(provider);
  }

  @Override public RelMetadataQuery get() {
    Function<RelMetadataQuery, IRelMetadataQuery> supplier = rmq -> {
      LambdaIRMQ lambdas = new LambdaIRMQ(handlerCache, rmq);
      CanonicalizingIRMQ nullCanonicalizingRMQ = new CanonicalizingIRMQ(lambdas);
      return nullCanonicalizingRMQ;
    };
    return new DelegatingRelMetadataQuery(supplier);
  }

  public static void main(String[] args) throws Exception {
    LambdaMetadataSupplier supplier = new LambdaMetadataSupplier();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelOptSchema schema = SqlValidatorUtil.createSingleTableCatalogReader(
        false,
        "t1",
        typeFactory,
        typeFactory.createStructType(
            Arrays.asList(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.BIGINT)),
            Arrays.asList("foo", "bar")
        )
    );

    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    cluster.setMetadataQuerySupplier(supplier);
    
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, schema);
    RelNode project = relBuilder.scan("t1")
        .project(relBuilder.field(1))
        .build();
    RelMetadataQuery mq = cluster.getMetadataQuery();

    //LambdaRMQ.ColumnOrigins<RelNode> meth = lg.get(Project.class, LambdaRMQ.ColumnOrigins.class).get(0);
    Set<RelColumnOrigin> origins = mq.getColumnOrigins(project, 0);
    origins.stream().forEach(System.out::println);
  }

  public interface LambdaProvider {
    <R extends RelNode, T> List<T> get(Class<R> relnodeClass, Class<T> handlerClass) throws
        ExecutionException;
  }
}
