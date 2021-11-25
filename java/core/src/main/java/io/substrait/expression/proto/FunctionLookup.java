package io.substrait.expression.proto;

import io.substrait.function.SimpleExtension;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a mapping between function anchors and function references. Generates references for new anchors.
 */
public class FunctionLookup {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionLookup.class);

  private final BidiMap<Integer, SimpleExtension.FunctionAnchor> map = new BidiMap<>();
  private int counter = -1;

  public int getFunctionReference(SimpleExtension.ScalarFunctionVariant declaration) {
    Integer i = map.reverseGet(declaration.getAnchor());
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    map.put(counter, declaration.getAnchor());
    return counter;
  }

  public SimpleExtension.ScalarFunctionVariant getScalarFunction(int reference, SimpleExtension.ExtensionCollection extensions) {
    var anchor = map.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException("Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getScalarFunction(anchor);
  }

  public SimpleExtension.AggregateFunctionVariant getAggregateFunction(int reference, SimpleExtension.ExtensionCollection extensions) {
    var anchor = map.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException("Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getAggregateFunction(anchor);
  }

  /**
   * We don't depend on guava...
   */
  private static class BidiMap<T1, T2> {

    private final Map<T1, T2> forwardMap = new HashMap<>();
    private final Map<T2, T1> reverseMap = new HashMap<>();

    public T2 get(T1 t1) {
      return forwardMap.get(t1);
    }

    public T1 reverseGet(T2 t2) {
      return reverseMap.get(t2);
    }

    public void put(T1 t1, T2 t2) {
      forwardMap.put(t1, t2);
      reverseMap.put(t2, t1);
    }
  }
}
