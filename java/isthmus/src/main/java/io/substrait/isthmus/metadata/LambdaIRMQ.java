package io.substrait.isthmus.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

class LambdaIRMQ implements IRelMetadataQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LambdaIRMQ.class);

  private final RelMetadataQuery top;
  private final LambdaHandlerCache handlerCache;
  private final Table<RelNode, Object, Object> metadataCache = HashBasedTable.create();

  public LambdaIRMQ(LambdaHandlerCache handlerCache, RelMetadataQuery top) {
    this.top = top;
    this.handlerCache = handlerCache;
  }

  private static final RelNode recurseDelegates(RelNode r) {
    while (r instanceof DelegatingMetadataRel) {
      r = ((DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    return r;
  }

  public boolean clearCache(RelNode rel) {
    Map<Object, Object> row = metadataCache.row(rel);
    if (row.isEmpty()) {
      return false;
    }

    row.clear();
    return true;
  }

  private <T> T findLambdas(Class<? extends RelNode> clazz, Class<?> methodInterface) {
    try {
      return (T) handlerCache.get(clazz, methodInterface);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if(cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  protected final  <T, A extends Arg0Lambda> T retrieve(RelNode r, Class<A> methodInterface) {
    r = recurseDelegates(r);
    final Object key = methodInterface;
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r, methodInterface);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends Arg0Lambda> T invoke(RelNode r, Class<A> methodInterface) {
    List<Arg0Lambda<RelNode, T>> lambdas = findLambdas(r.getClass(), methodInterface);
    if (lambdas.isEmpty()) {
      return null;
    }

    for (Arg0Lambda<RelNode, T> lambda : lambdas) {
      T val = lambda.call(r, top);
      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private <T, A extends Arg1Lambda> T retrieve(RelNode r, Class<A> methodInterface, Object o0){
    r = recurseDelegates(r);
    final List key;
    key = org.apache.calcite.runtime.FlatLists.of(methodInterface, keyifyArg(o0));
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r, methodInterface, o0);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends Arg1Lambda> T invoke(RelNode r, Class<A> methodInterface, Object o0) {
    List<Arg1Lambda<RelNode, Object, T>> lambdas = findLambdas(r.getClass(), methodInterface);;
    if (lambdas.isEmpty()) {
      return null;
    }

    for (Arg1Lambda<RelNode, Object, T> lambda : lambdas) {
      T val = lambda.call(r, top, o0);
      if (val != null) {
        return val;
      }
    }

    return null;
  }


  private <T, A extends Arg2Lambda> T retrieve(RelNode r, Class<A> methodInterface, Object o0, Object o1){
    r = recurseDelegates(r);
    final List key;
    key = org.apache.calcite.runtime.FlatLists.of(methodInterface, keyifyArg(o0), keyifyArg(o1));
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r,methodInterface, o0, o1);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends Arg2Lambda> T invoke(RelNode r, Class<A> methodInterface, Object o0, Object o1) {
    List<Arg2Lambda<RelNode, Object, Object, T>> lambdas = findLambdas(r.getClass(), methodInterface);;
    if (lambdas.isEmpty()) {
      return null;
    }
    
    for (Arg2Lambda<RelNode, Object, Object, T> lambda : lambdas) {
      T val = lambda.call(r, top, o0, o1);
      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private static final Object keyifyArg(Object arg) {
    if(arg instanceof RexNode) {
      // RexNodes need to be converted to strings to support use in a key.
      return arg.toString();
    }
    return arg;
  }

  private final Object check(RelNode r, Object key) {
    final Object v = metadataCache.get(r, key);
    if( v == null) {
      return null;
    }
    if (v == NullSentinel.ACTIVE) {
      throw new CyclicMetadataException();
    }
    if (v == NullSentinel.INSTANCE) {
      return null;
    }
    return v;
  }


  public @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(final RelNode rel) {
    return retrieve(rel, NodeTypes.class);
  }

  public Double getRowCount(final RelNode rel) {
    return retrieve(rel, RowCount.class);
  }

  public @Nullable Double getMaxRowCount(final RelNode rel) {
    return retrieve(rel, MaxRowCount.class);
  }

  public @Nullable Double getMinRowCount(final RelNode rel) {
    return retrieve(rel, MinRowCount.class);
  }

  public @Nullable RelOptCost getCumulativeCost(final RelNode rel) {
    return retrieve(rel, CumulativeCost.class);
  }

  public @Nullable RelOptCost getNonCumulativeCost(final RelNode rel) {
    return retrieve(rel, NonCumulativeCost.class);
  }

  public @Nullable Double getPercentageOriginalRows(final RelNode rel) {
    return retrieve(rel, PercentageOriginalRows.class);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(final RelNode rel, final int column) {
    return retrieve(rel, ColumnOrigins.class, column);
  }

  public @Nullable Set<RexNode> getExpressionLineage(final RelNode rel, final RexNode expression) {
    return retrieve(rel, ExpressionLineage.class, expression);
  }

  public @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(final RelNode rel) {
    return retrieve(rel, TableReferences.class);
  }

  public @Nullable Double getSelectivity(final RelNode rel, @Nullable final RexNode predicate) {
    return retrieve(rel, Selectivity.class, predicate);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(final RelNode rel, final boolean ignoreNulls) {
    return retrieve(rel, UniqueKeys.class, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(final RelNode rel, final ImmutableBitSet columns, final boolean ignoreNulls) {
    return retrieve(rel, ColumnsUnique.class, columns, ignoreNulls);
  }

  public @Nullable ImmutableList<RelCollation> collations(final RelNode rel) {
    return retrieve(rel, Collations.class);
  }

  public @Nullable Double getPopulationSize(final RelNode rel, final ImmutableBitSet groupKey) {
    return retrieve(rel, PopulationSize.class, groupKey);
  }

  public @Nullable Double getAverageRowSize(final RelNode rel) {
    return retrieve(rel, AverageRowSize.class);
  }

  public @Nullable List<@Nullable Double> getAverageColumnSizes(final RelNode rel) {
    return retrieve(rel, AverageColumnSizes.class);
  }

  public @Nullable Boolean isPhaseTransition(final RelNode rel) {
    return retrieve(rel, PhaseTransition.class);
  }

  public @Nullable Integer splitCount(final RelNode rel) {
    return retrieve(rel, SplitCount.class);
  }

  public @Nullable Double memory(final RelNode rel) {
    return retrieve(rel, Memory.class);
  }

  public @Nullable Double cumulativeMemoryWithinPhase(final RelNode rel) {
    return retrieve(rel, CumulativeMemoryWithinPhase.class);
  }

  public @Nullable Double cumulativeMemoryWithinPhaseSplit(final RelNode rel) {
    return retrieve(rel, CumulativeMemoryWithinPhaseSplit.class);
  }

  public @Nullable Double getDistinctRowCount(final RelNode rel, final ImmutableBitSet groupKey, @Nullable final RexNode predicate) {
    return retrieve(rel, DistinctRowCount.class, groupKey, predicate);
  }

  public RelOptPredicateList getPulledUpPredicates(final RelNode rel) {
    return retrieve(rel, PulledUpPredicates.class);
  }

  public @Nullable RelOptPredicateList getAllPredicates(final RelNode rel) {
    return retrieve(rel, AllPredicates.class);
  }

  public Boolean isVisibleInExplain(final RelNode rel, final SqlExplainLevel explainLevel) {
    return retrieve(rel, VisibleInExplain.class, explainLevel);
  }

  public @Nullable RelDistribution getDistribution(final RelNode rel) {
    return retrieve(rel, Distribution.class);
  }

  public @Nullable RelOptCost getLowerBoundCost(final RelNode rel, final VolcanoPlanner planner) {
    return retrieve(rel, LowerBoundCost.class, planner);
  }

  interface NodeTypes<R extends RelNode> extends Arg0Lambda<R, Multimap<Class<? extends RelNode>, RelNode>> {}
  interface AverageColumnSizes<R extends RelNode> extends Arg0Lambda<R, List<@Nullable Double>> {}
  interface RowCount<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface MaxRowCount<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface MinRowCount<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface CumulativeCost<R extends RelNode> extends Arg0Lambda<R, RelOptCost> {}
  interface NonCumulativeCost<R extends RelNode> extends Arg0Lambda<R, RelOptCost> {}
  interface PercentageOriginalRows<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface ColumnOrigins<R extends RelNode> extends Arg1Lambda<R, Integer, Set<RelColumnOrigin>> {}
  interface ExpressionLineage<R extends RelNode> extends Arg1Lambda<R, RexNode, Set<RexNode>> {}
  interface TableReferences<R extends RelNode> extends Arg0Lambda<R, Set<RexTableInputRef.RelTableRef>> {}
  interface Selectivity<R extends RelNode> extends Arg1Lambda<R, RexNode, Double> {}
  interface UniqueKeys<R extends RelNode> extends Arg1Lambda<R, Boolean, Set<ImmutableBitSet>> {}
  interface ColumnsUnique<R extends RelNode> extends Arg2Lambda<R, ImmutableBitSet, Boolean, Boolean> {}
  interface Collations<R extends RelNode> extends Arg0Lambda<R, ImmutableList<RelCollation>> {}
  interface PopulationSize<R extends RelNode> extends Arg1Lambda<R, ImmutableBitSet, Double> {}
  interface AverageRowSize<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface PhaseTransition<R extends RelNode> extends Arg0Lambda<R, Boolean> {}
  interface SplitCount<R extends RelNode> extends Arg0Lambda<R, Integer> {}
  interface Memory<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface CumulativeMemoryWithinPhase<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface CumulativeMemoryWithinPhaseSplit<R extends RelNode> extends Arg0Lambda<R, Double> {}
  interface DistinctRowCount<R extends RelNode> extends Arg2Lambda<R, ImmutableBitSet, @Nullable RexNode, Double> {}
  interface PulledUpPredicates<R extends RelNode> extends Arg0Lambda<R, RelOptPredicateList> {}
  interface AllPredicates<R extends RelNode> extends Arg0Lambda<R, RelOptPredicateList> {}
  interface VisibleInExplain<R extends RelNode> extends Arg1Lambda<R, SqlExplainLevel, Boolean> {}
  interface Distribution<R extends RelNode> extends Arg0Lambda<R, RelDistribution> {}
  interface LowerBoundCost<R extends RelNode> extends Arg1Lambda<R, VolcanoPlanner, RelOptCost> {}

  @FunctionalInterface
  interface Arg0Lambda<T extends RelNode, R> {
    R call(T rel, RelMetadataQuery mq);

  }

  @FunctionalInterface
  interface Arg1Lambda<T extends RelNode, A0, R> {
    R call(T rel, RelMetadataQuery mq, A0 arg0);
  }

  @FunctionalInterface
  interface Arg2Lambda<T extends RelNode, A0, A1, R> {
    R call(T rel, RelMetadataQuery mq, A0 arg0, A1 arg1);
  }
}
