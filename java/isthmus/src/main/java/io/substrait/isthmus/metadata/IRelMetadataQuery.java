package io.substrait.isthmus.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface IRelMetadataQuery {

  @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(final RelNode rel);
  @Nullable Double getMaxRowCount(final RelNode rel);
  @Nullable Double getMinRowCount(final RelNode rel);
  @Nullable RelOptCost getCumulativeCost(final RelNode rel);
  @Nullable RelOptCost getNonCumulativeCost(final RelNode rel);
  @Nullable Double getPercentageOriginalRows(final RelNode rel);
  @Nullable Set<RelColumnOrigin> getColumnOrigins(final RelNode rel, final int column);
  @Nullable Set<RexNode> getExpressionLineage(final RelNode rel, final RexNode expression);
  @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(final RelNode rel);
  @Nullable Double getSelectivity(final RelNode rel, @Nullable final RexNode predicate);
  @Nullable Set<ImmutableBitSet> getUniqueKeys(final RelNode rel, final boolean ignoreNulls);
  @Nullable Boolean areColumnsUnique(final RelNode rel, final ImmutableBitSet columns, final boolean ignoreNulls);
  @Nullable ImmutableList<RelCollation> collations(final RelNode rel);
  @Nullable Double getPopulationSize(final RelNode rel, final ImmutableBitSet groupKey);
  @Nullable Double getAverageRowSize(final RelNode rel);
  @Nullable List<@Nullable Double> getAverageColumnSizes(final RelNode rel);
  @Nullable Boolean isPhaseTransition(final RelNode rel);
  @Nullable Integer splitCount(final RelNode rel);
  @Nullable Double memory(final RelNode rel);
  @Nullable Double cumulativeMemoryWithinPhase(final RelNode rel);
  @Nullable Double cumulativeMemoryWithinPhaseSplit(final RelNode rel);
  @Nullable Double getDistinctRowCount(final RelNode rel, final ImmutableBitSet groupKey, @Nullable final RexNode predicate);
  @Nullable RelOptPredicateList getAllPredicates(final RelNode rel);
  @Nullable RelDistribution getDistribution(final RelNode rel);
  @Nullable RelOptCost getLowerBoundCost(final RelNode rel, final VolcanoPlanner planner);

  Double getRowCount(final RelNode rel);
  RelOptPredicateList getPulledUpPredicates(final RelNode rel);
  Boolean isVisibleInExplain(final RelNode rel, final SqlExplainLevel explainLevel);

  boolean clearCache(RelNode rel);

  default List<@Nullable Double> getAverageColumnSizesNotNull(final RelNode rel) {
    final @Nullable List<@Nullable Double> averageColumnSizes = getAverageColumnSizes(rel);
    return averageColumnSizes == null
        ? Collections.nCopies(rel.getRowType().getFieldCount(), null)
        : averageColumnSizes;
  }

  default RelDistribution distribution(final RelNode rel) {
    return getDistribution(rel);
  }

  default @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    return getUniqueKeys(rel, false);
  }
  default @Nullable Boolean areRowsUnique(final RelNode rel, final boolean ignoreNulls) {
    Double maxRowCount = this.getMaxRowCount(rel);
    if (maxRowCount != null && maxRowCount <= 1D) {
      return true;
    }
    final ImmutableBitSet columns =
        ImmutableBitSet.range(rel.getRowType().getFieldCount());
    return areColumnsUnique(rel, columns, ignoreNulls);
  }

  default @Nullable Boolean areRowsUnique(final RelNode rel) {
    return areRowsUnique(rel, false);
  }

  default @Nullable Boolean areColumnsUnique(final RelNode rel, final ImmutableBitSet columns) {
    return areColumnsUnique(rel, columns, false);
  }
  default @Nullable RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    final Set<RelColumnOrigin> origins = getColumnOrigins(rel, column);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    final RelColumnOrigin origin = Iterables.getOnlyElement(origins);
    return origin;
  }

  default @Nullable RelOptTable getTableOrigin(final RelNode rel) {
    // Determine the simple origin of the first column in the
    // RelNode.  If it's simple, then that means that the underlying
    // table is also simple, even if the column itself is derived.
    if (rel.getRowType().getFieldCount() == 0) {
      return null;
    }
    final Set<RelColumnOrigin> colOrigins = getColumnOrigins(rel, 0);
    if (colOrigins == null || colOrigins.size() == 0) {
      return null;
    }
    return colOrigins.iterator().next().getOriginTable();
  }

}
