package io.substrait.isthmus;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageLite;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdLowerBoundCost;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBeans;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RegisterAtRuntime implements Feature {
  public void beforeAnalysis(BeforeAnalysisAccess access) {
    try {
      Reflections substrait = new Reflections("io.substrait");
      // cli picocli
      register(PlanEntryPoint.class);
      
      //protobuf items
      registerByParent(substrait, GeneratedMessageV3.class);
      registerByParent(substrait, MessageLite.Builder.class);

      // calcite items
      Reflections calcite = new Reflections("org.apache.calcite", new FieldAnnotationsScanner(), new SubTypesScanner());
      register(BuiltInMetadata.class);

      registerByParent(calcite, Metadata.class);
      registerByParent(calcite, MetadataHandler.class);
      registerByParent(calcite, Resources.Element.class);

      Arrays.asList(RelMdPercentageOriginalRows.class,
          RelMdColumnOrigins.class,
          RelMdExpressionLineage.class,
          RelMdTableReferences.class,
          RelMdNodeTypes.class,
          RelMdRowCount.class,
          RelMdMaxRowCount.class,
          RelMdMinRowCount.class,
          RelMdUniqueKeys.class,
          RelMdColumnUniqueness.class,
          RelMdPopulationSize.class,
          RelMdSize.class,
          RelMdParallelism.class,
          RelMdDistribution.class,
          RelMdLowerBoundCost.class,
          RelMdMemory.class,
          RelMdDistinctRowCount.class,
          RelMdSelectivity.class,
          RelMdExplainVisibility.class,
          RelMdPredicates.class,
          RelMdAllPredicates.class,
          RelMdCollation.class)
          .forEach(RegisterAtRuntime::register);

      RuntimeReflection.register(Resources.class);
      RuntimeReflection.register(SqlValidatorException.class);
      //registerByParent(calcite, RelOptRule.class);
      //registerByParent(calcite, RelRule.Config.class);

      // register everything that has a immutable beans property
//      for(Class<?> c : calcite.getFieldsAnnotatedWith(ImmutableBeans.Property.class)
//          .stream().map(f -> f.getDeclaringClass()).collect(Collectors.toSet())) {
//        register(c);
//      }
      Arrays.stream(BuiltInMethod.values()).forEach(c -> {
        if (c.field != null) RuntimeReflection.register(c.field);
        if (c.constructor != null) RuntimeReflection.register(c.constructor);
        if (c.method != null) RuntimeReflection.register(c.method);
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void register(Class<?> c) {
    RuntimeReflection.register(c);

    RuntimeReflection.register(c.getDeclaredConstructors());
    RuntimeReflection.register(c.getDeclaredFields());
    RuntimeReflection.register(c.getDeclaredMethods());


    RuntimeReflection.register(c.getConstructors());
    RuntimeReflection.register(c.getFields());
    RuntimeReflection.register(c.getMethods());
  }
  
  private static void registerByParent(Reflections reflections, Class<?> c) {

    reflections.getSubTypesOf(c).stream().forEach(RegisterAtRuntime::register);
  }


  ;
}