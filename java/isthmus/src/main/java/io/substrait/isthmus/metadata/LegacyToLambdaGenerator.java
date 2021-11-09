package io.substrait.isthmus.metadata;

import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.common.primitives.Primitives;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Create a set of lambda handlers via reflection.
 *
 * This does reflection => lambda conversion based on a set of singleton objects
 * with appropriate signatures, similar to how ReflectiveRelMetadataProvider works.
 * Any class that can be discovered using ReflectiveRelMetadataProvider should also be consumable
 * using this class. Ultimately, the goal may be to move to direct lambda registration as opposed to
 * the old system of partial reflection discovery.
 */
public class LegacyToLambdaGenerator implements LambdaMetadataSupplier.LambdaProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyToLambdaGenerator.class);

  public static final ImmutableList<Source> DEFAULT_SOURCES = ImmutableList.<Source>builder()
      .add(s(RelMdColumnOrigins.class, "getColumnOrigins", LambdaIRMQ.ColumnOrigins.class))
      .add(s(RelMdPercentageOriginalRows.class, "getPercentageOriginalRows", LambdaIRMQ.PercentageOriginalRows.class))
      .add(s(RelMdExpressionLineage.class, "getExpressionLineage", LambdaIRMQ.ExpressionLineage.class))
      .add(s(RelMdTableReferences.class, "getTableReferences", LambdaIRMQ.TableReferences.class))
      .add(s(RelMdNodeTypes.class, "getNodeTypes", LambdaIRMQ.NodeTypes.class))
      .add(s(RelMdRowCount.class, "getRowCount", LambdaIRMQ.RowCount.class))
      .add(s(RelMdMaxRowCount.class, "getMaxRowCount", LambdaIRMQ.MaxRowCount.class))
      .add(s(RelMdMinRowCount.class, "getMinRowCount", LambdaIRMQ.MinRowCount.class))
      .add(s(RelMdUniqueKeys.class, "getUniqueKeys", LambdaIRMQ.UniqueKeys.class))
      .add(s(RelMdColumnUniqueness.class, "areColumnsUnique", LambdaIRMQ.ColumnsUnique.class))
      .add(s(RelMdPopulationSize.class, "getPopulationSize", LambdaIRMQ.PopulationSize.class))
      .add(s(RelMdSize.class, "averageRowSize", LambdaIRMQ.AverageRowSize.class))
      .add(s(RelMdSize.class, "averageColumnSizes", LambdaIRMQ.AverageColumnSizes.class))
      .add(s(RelMdParallelism.class, "isPhaseTransition", LambdaIRMQ.PhaseTransition.class))
      .add(s(RelMdParallelism.class, "splitCount", LambdaIRMQ.SplitCount.class))
      .add(s(RelMdDistribution.class, "distribution", LambdaIRMQ.Distribution.class))
      .add(s(RelMdLowerBoundCost.class, "getLowerBoundCost", LambdaIRMQ.LowerBoundCost.class))
      .add(s(RelMdMemory.class, "memory", LambdaIRMQ.Memory.class))
      .add(s(RelMdDistinctRowCount.class, "getDistinctRowCount", LambdaIRMQ.DistinctRowCount.class))
      .add(s(RelMdSelectivity.class, "getSelectivity", LambdaIRMQ.Selectivity.class))
      .add(s(RelMdExplainVisibility.class, "isVisibleInExplain", LambdaIRMQ.VisibleInExplain.class))
      .add(s(RelMdPredicates.class, "getPredicates", LambdaIRMQ.PulledUpPredicates.class))
      .add(s(RelMdAllPredicates.class, "getAllPredicates", LambdaIRMQ.AllPredicates.class))
      .add(s(RelMdCollation.class, "collations", LambdaIRMQ.Collations.class))
      .build();

  // Maintains a list of lambdas associated with each RelNode + MetadataInterface pair.
  private final Table<Class<?>, Class<?>, List<Object>> items;

  // Maintains an ordered list of relnode interfaces for a particular relnode. This is done so we don't have to do the enumeration for each metadata pattern.
  private final LoadingCache<Class<? extends RelNode>, List<Class<? extends RelNode>>> classImplementations = CacheBuilder.newBuilder().build(
      new CacheLoader<Class<? extends RelNode>, List<Class<? extends RelNode>>>() {
        @Override public List<Class<? extends RelNode>> load(final Class<? extends RelNode> key) throws Exception {
          return getImplements((Class<? extends RelNode>) key);
        }
      });
  
  public LegacyToLambdaGenerator(Source... sources) {
    this(Arrays.asList(sources));
  }

  public LegacyToLambdaGenerator() {
    this(DEFAULT_SOURCES);
  }
  
  public LegacyToLambdaGenerator(Iterable<Source> sources) {

    // build a list of lambdas for a given class.
    HashBasedTable<Class<?> /** relnode **/, Class<?> /** handler class **/, List<Object>> table = HashBasedTable.create();

    for(Source source : sources) {
      Map<Class<?>, Object> lambdas = lamb(source);
      for (Map.Entry<Class<?>, Object> e : lambdas.entrySet()) {
        List<Object> objects = table.get(e.getKey(), source.lambdaClass);
        if (objects == null) {
          objects = new ArrayList<>();
          table.put(e.getKey(), source.lambdaClass, objects);
        }
        objects.add(e.getValue());
      }
    }

    this.items = table;
  }


  /**
   * For a given source, generate a map of specific relnode classes to lambdas.
   * @param source
   * @return
   */
  private Map<Class<?>, Object> lamb(Source source) {
    Class<?> clazz = source.singleton.getClass();
    String name = source.sourceMethod;
    Class<?> arg = source.lambdaClass;
    try {
      final Object singleton = source.singleton;

      List<Method> methods = Arrays.stream(clazz.getMethods()).filter(f -> f.getName().equals(name)).toList();
      Map<Class<?>, Object> output = new HashMap<>();
      for (Method reflectionMethod : methods) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType callSiteType = MethodType.methodType(arg, clazz);
        MethodType functionalMethod;
        MethodType delegateMethod;

        // generate methods based on number of arguments.
        if(LambdaIRMQ.Arg0Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class, reflectionMethod.getParameterTypes()[1]);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(), reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class);
        } else if(LambdaIRMQ.Arg1Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class, reflectionMethod.getParameterTypes()[1], Object.class);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(), reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class, reflectionMethod.getParameterTypes()[2]);
          if (reflectionMethod.getParameterTypes()[2].isPrimitive()){
            delegateMethod = delegateMethod.changeParameterType(2, Primitives.wrap(reflectionMethod.getParameterTypes()[2]));
          }
        } else if(LambdaIRMQ.Arg2Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class, reflectionMethod.getParameterTypes()[1], Object.class, Object.class);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(), reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class, reflectionMethod.getParameterTypes()[2], reflectionMethod.getParameterTypes()[3]);
          if (reflectionMethod.getParameterTypes()[2].isPrimitive()){
            delegateMethod = delegateMethod.changeParameterType(2, Primitives.wrap(reflectionMethod.getParameterTypes()[2]));
          }
          if (reflectionMethod.getParameterTypes()[3].isPrimitive()){
            delegateMethod = delegateMethod.changeParameterType(3, Primitives.wrap(reflectionMethod.getParameterTypes()[3]));
          }
        } else {
          throw new IllegalStateException();
        }
        MethodHandle delegate = lookup.unreflect(reflectionMethod);
        CallSite callSite = LambdaMetafactory.metafactory(lookup, "call", callSiteType, functionalMethod, delegate, delegateMethod);
        Object val = callSite.getTarget().bindTo(singleton).invoke();
        output.put(reflectionMethod.getParameterTypes()[0], val);
      }
      return output;
    } catch (Throwable ex){
      throw new RuntimeException(ex);
    }
  }

  /** Describes a source of metadata methods **/
  public static class Source {
    private final Object singleton;
    private final String sourceMethod;
    private final Class<?> lambdaClass;

    public Source(final Class<?> privateSingletonClass, final String sourceMethod, final Class<?> lambdaClass) {
      this(privateSingleton(privateSingletonClass), sourceMethod, lambdaClass);
    }

    public Source(final Object singleton, final String sourceMethod, final Class<?> lambdaClass) {
      this.singleton = singleton;
      this.sourceMethod = sourceMethod;
      this.lambdaClass = lambdaClass;
    }

    private static Object privateSingleton(Class<?> clazz) {
      try {
        final Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        Constructor<?> noArg = Arrays.stream(constructors).filter(c -> c.getParameterTypes().length == 0).findFirst().get();
        noArg.setAccessible(true);
        return noArg.newInstance();
      } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
        throw new RuntimeException(e);
      }
    }

    public static Source of(Class<?> sourceClass, String sourceMethod, Class<?> lambdaClass) {
      return new Source(sourceClass, sourceMethod, lambdaClass);
    }

    public static Source of(Object instance, String sourceMethod, Class<?> lambdaClass) {
      return new Source(instance, sourceMethod, lambdaClass);
    }
  }

  @Override public <R extends RelNode, T> List<T> get(Class<R> relnodeClass, Class<T> handlerClass) throws ExecutionException {
    List<Class<? extends RelNode>> classes = classImplementations.get(relnodeClass);
    ImmutableList.Builder<T> handlers = ImmutableList.builder();
    for(Class<?> clazz : classes) {
      List<Object> partialHandlers = items.get(clazz, handlerClass);
      if (partialHandlers == null) {
        continue;
      }

      for(Object o : partialHandlers) {
        handlers.add((T) o);
      }
    }
    return handlers.build();
  }

  private static Source s(Class<?> sourceClass, String sourceMethod, Class<?> lambdaClass) {
    return Source.of(sourceClass, sourceMethod, lambdaClass);
  }

  private static Source s(Object instance, String sourceMethod, Class<?> lambdaClass) {
    return Source.of(instance, sourceMethod, lambdaClass);
  }

  /**
   * Generate a list of interfaces/classes that this node implements, from nearest to furthest.
   * @param base
   * @return
   */
  private static List<Class<? extends RelNode>> getImplements(Class<? extends RelNode> base) {
    ImmutableList.Builder<Class<? extends RelNode>> builder = ImmutableList.builder();
    addImplements(base, builder);
    return builder.build();
  }

  private static void addImplements(Class<?> base, ImmutableList.Builder<Class<? extends RelNode>> builder) {
    if (base == null || !RelNode.class.isAssignableFrom(base)) {
      return;
    }
    builder.add((Class<? extends RelNode>) base);
    Arrays.stream(base.getInterfaces()).forEach(c -> addImplements(c, builder));
    addImplements(base.getSuperclass(), builder);
  }

}
