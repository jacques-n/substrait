package io.substrait.isthmus.metadata;

import org.apache.calcite.rel.RelNode;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Maintains a cache of handlers for each discovered RelNode/lambda interface combination.
 */
class LambdaHandlerCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LambdaHandlerCache.class);

  private final LambdaMetadataSupplier.LambdaProvider provider;
  private final LoadingCache<Class<? extends RelNode>, LoadingCache<Class, Object>> cache;

  public LambdaHandlerCache(final LambdaMetadataSupplier.LambdaProvider provider) {
    this.provider = provider;
    this.cache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Class, LoadingCache<Class, Object>>() {
                 @Override public LoadingCache<Class, Object> load(final Class relNodeClazz)
                     throws Exception {
                   return CacheBuilder.<Class, Object>newBuilder().build(
                       new CacheLoader<Class, Object>() {
                         @Override public Object load(final Class lambdaClazz) throws Exception {
                           return provider.get((Class<? extends RelNode>) relNodeClazz, lambdaClazz);
                         }
                       });
                 }
               }
        );
  }

  public <R extends RelNode, T> List<T> get(Class<R> relNodeClazz, Class<T> lambdaClazz) throws ExecutionException {
    return (List<T>) cache.get(relNodeClazz).get(lambdaClazz);
  }
  
}
