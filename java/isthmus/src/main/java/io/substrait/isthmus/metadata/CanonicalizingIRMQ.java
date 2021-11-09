package io.substrait.isthmus.metadata;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Canonicalizes methods from null to other standard outputs.
 */
public class CanonicalizingIRMQ extends DelegatingIRMQ {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CanonicalizingIRMQ.class);

  public CanonicalizingIRMQ(IRelMetadataQuery delegate) {
    super(delegate);
  }

  @Override public @Nullable RelDistribution distribution(final RelNode rel) {
    RelDistribution distribution = super.distribution(rel);
    if (distribution == null) {
      return RelDistributions.ANY;
    }
    return distribution;
  }

  public Double getRowCount(RelNode rel) {
    return RelMdUtil.validateResult(castNonNull(super.getRowCount(rel)));
  }

  public @Nullable Double getPopulationSize(RelNode rel, ImmutableBitSet groupKey) {
    return RelMdUtil.validateResult(super.getPopulationSize(rel, groupKey));
  }

  public @Nullable Double getPercentageOriginalRows(RelNode rel) {
    Double result = super.getPercentageOriginalRows(rel);
    return RelMdUtil.validatePercentage(result);
  }

  public @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate) {
    return RelMdUtil.validatePercentage(super.getSelectivity(rel, predicate));
  }

  public @Nullable Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      @Nullable RexNode predicate) {
    return RelMdUtil.validateResult(super.getDistinctRowCount(rel, groupKey, predicate));
  }

  public RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    RelOptPredicateList result = super.getPulledUpPredicates(rel);
    return result != null ? result : RelOptPredicateList.EMPTY;
  }

  public Boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    Boolean b = super.isVisibleInExplain(rel, explainLevel);
    return b == null || b;
  }
}
