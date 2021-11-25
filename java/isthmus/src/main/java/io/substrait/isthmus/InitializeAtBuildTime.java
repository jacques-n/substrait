package io.substrait.isthmus;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import io.substrait.isthmus.metadata.LambdaMetadataSupplier;

import java.util.function.Supplier;

public class InitializeAtBuildTime {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(
      InitializeAtBuildTime.class);

  static Supplier<RelMetadataQuery> SUPPLIER = LambdaMetadataSupplier.instance();
}
