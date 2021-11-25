package io.substrait.isthmus;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.function.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ToTypeString;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.substrait.util.Util;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;


import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FunctionConverter implements RexExpressionConverter.CallConverter {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);

  // Static list of signature mapping between Calcite SQL operators and Substrait base function names.
  private static final ImmutableList<Sig> SIGS;

  private final Map<SqlOperator, FunctionFinder> signatures;
  private final RelDataTypeFactory typeFactory;


  static {
    SIGS = ImmutableList.<Sig>builder().add(
        s(SqlStdOperatorTable.PLUS, "add"),
        s(SqlStdOperatorTable.MINUS, "minus"),
        s(SqlStdOperatorTable.AND),
        s(SqlStdOperatorTable.OR),
        s(SqlStdOperatorTable.NOT)
    ).build();
  }

  public FunctionConverter(List<SimpleExtension.ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.EMPTY_LIST, typeFactory);
  }

  public FunctionConverter(List<SimpleExtension.ScalarFunctionVariant> functions, List<Sig> additionalSignatures, RelDataTypeFactory typeFactory) {
    var signatures = new ArrayList<Sig>(SIGS.size() + additionalSignatures.size());
    signatures.addAll(additionalSignatures);
    signatures.addAll(SIGS);
    this.typeFactory = typeFactory;

    var alm = ArrayListMultimap.<String, SimpleExtension.ScalarFunctionVariant>create();
    for (var f : functions) {
      alm.put(f.name().toLowerCase(Locale.ROOT), f);
    }

    Map<String, SqlOperator> calciteOperators = signatures.stream().collect(Collectors.toMap(Sig::name, Sig::operator));
    var matcherMap = new IdentityHashMap<SqlOperator, FunctionFinder>();
    for (String key : alm.keySet()) {
      SqlOperator operator = calciteOperators.get(key);
      if (operator == null) {
        continue;
      }

      var implList = alm.get(key);
      if (implList == null || implList.isEmpty()) {
        continue;
      }


      matcherMap.put(operator, new FunctionFinder(key, operator, implList));
    }

    this.signatures = matcherMap;
  }

  public static Sig s(SqlOperator operator, String substraitName) {
    return new Sig(operator, substraitName.toLowerCase(Locale.ROOT));
  }

  public static Sig s(SqlOperator operator) {
    return s(operator, operator.getName().toLowerCase(Locale.ROOT));
  }

  @Override
  public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder m = signatures.get(call.op);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.getOperands().size())) {
      return Optional.empty();
    }
    
    return m.attemptScalarMatch(call, topLevelConverter);
  }

  record Sig(SqlOperator operator, String name) {}

  private class FunctionFinder {
    private final String name;
    private final SqlOperator operator;
    private final List<SimpleExtension.ScalarFunctionVariant> functions;
    private final Map<String, SimpleExtension.ScalarFunctionVariant> directMap;
    private final SignatureMatcher matcher;
    private final Optional<SingularArgumentMatcher> singularInputType;
    private final Util.IntRange argRange;

    public FunctionFinder(String name, SqlOperator operator, List<SimpleExtension.ScalarFunctionVariant> functions) {
      this.name = name;
      this.operator = operator;
      this.functions = functions;
      this.argRange = Util.IntRange.of(
          functions.stream().mapToInt(t -> t.getRange().getStartInclusive()).min().getAsInt(),
          functions.stream().mapToInt(t -> t.getRange().getEndExclusive()).max().getAsInt());
      this.matcher = getSignatureMatcher(operator, functions);
      this.singularInputType = getSingularInputType(functions);
      var directMap = ImmutableMap.<String, SimpleExtension.ScalarFunctionVariant>builder();
      for (var func : functions) {
        String key = func.key();
        directMap.put(key, func);
        if (func.requiredArguments().size() != func.args().size()) {
          directMap.put(SimpleExtension.Function.constructKey(name, func.requiredArguments()), func);
        }
      }
      this.directMap = directMap.build();
    }

    public boolean allowedArgCount(int count) {
      return argRange.within(count);
    }

    private static SignatureMatcher getSignatureMatcher(SqlOperator operator, List<SimpleExtension.ScalarFunctionVariant> functions) {
      // TODO: define up-converting matchers.
      return (a, b) -> Optional.empty();
    }

    /**
     * If some of the function variants for this function name have single, repeated argument type,
     * we will attempt to find matches using these patterns and least-restrictive casting.
     *
     * If this exists, the function finder will attempt to find a least-restrictive match using these.
     */
    private static Optional<SingularArgumentMatcher> getSingularInputType(List<SimpleExtension.ScalarFunctionVariant> functions) {
      List<SingularArgumentMatcher> matchers = new ArrayList<>();
      for (var f : functions) {

        // no need to do optional requirements since singular input only supports value arguments.
        if (f.requiredArguments().size() < 2) {
          continue;
        }

        ParameterizedType firstType = null;

        // determine if all the required arguments are the of the same type. If so,
        for (var a : f.requiredArguments()) {
          if (!(a instanceof SimpleExtension.ValueArgument)) {
            firstType = null;
            break;
          }

          var pt = ((SimpleExtension.ValueArgument) a).value();

          if (firstType == null) {
            firstType = pt;
          } else {

            // if the parametertized types match exactly, they are the same. (This means that their parameters need to match as well
            if (!firstType.equals(pt)) {
              firstType = null;
              break;
            }
          }
        }


        if (firstType != null) {
          matchers.add(singular(f, firstType));
        }

      }

      return switch(matchers.size()) {
        case 0 -> Optional.empty();
        case 1 -> Optional.of(matchers.get(0));
        default -> Optional.of(chained(matchers));
      };
    }

    public static SingularArgumentMatcher singular(SimpleExtension.ScalarFunctionVariant function, ParameterizedType type) {
      return (inputType, outputType) -> {
        var check = inputType.accept(new IgnoreNullableAndParameters(type));
        if (check) {
          return Optional.of(function);
        }
        return Optional.empty();
      };
    }

    public static SingularArgumentMatcher chained(List<SingularArgumentMatcher> matchers) {
      return (inputType, outputType) -> {
        for (var s : matchers) {
          var outcome = s.tryMatch(inputType, outputType);
          if (outcome.isPresent()) {
            return outcome;
          }
        }
        
        return Optional.empty();
      };
    }

    public Optional<Expression> attemptScalarMatch(RexCall call, Function<RexNode, Expression> topLevelConverter) {

      var operands = call.getOperands().stream().map(topLevelConverter).toList();
      var opTypes = operands.stream().map(Expression::getType).toList();

      var outputType = TypeConverter.convert(call.getType());

      // try to do a direct match
      var directMatchkey = SimpleExtension.Function.constructKeyFromTypes(name, opTypes);
      var variant = directMap.get(directMatchkey);
      if (variant != null) {
        variant.validateOutputType(operands, outputType);
        return Optional.of(generateBinding(variant, operands, outputType));
      }


      if (singularInputType.isPresent()) {
        RelDataType leastRestrictive = typeFactory.leastRestrictive(call.getOperands().stream().map(RexNode::getType).toList());
        if (leastRestrictive == null) {
          return Optional.empty();
        }
        Type type = TypeConverter.convert(leastRestrictive);
        var out = singularInputType.get().tryMatch(type, outputType);

        if (out.isPresent()) {
          var declaration = out.get();
          var coercedArgs = coerceArguments(operands, type);
          declaration.validateOutputType(coercedArgs, outputType);
          return Optional.of(generateBinding(out.get(), coercedArgs, outputType));
        }
      }
      return Optional.empty();
    }
  }

  /**
   * Coerced types according to an expected output type. Coercion is only done for type mismatches, not for nullability or parameter mismatches.
   */
  private List<Expression> coerceArguments(List<Expression> arguments, Type type) {
    return arguments.stream().map(a -> {
      var typeMatches = type.accept(new IgnoreNullableAndParameters(a.getType()));
      if (!typeMatches) {
        return ExpressionCreator.cast(type, a);
      }
      return a;
    }).toList();
  }

  private Expression generateBinding(SimpleExtension.ScalarFunctionVariant function, List<Expression> arguments, Type outputType) {
      return Expression.ScalarFunctionInvocation.builder()
          .outputType(outputType)
          .declaration(function)
          .addAllArguments(arguments)
          .build();
  }

  public interface SingularArgumentMatcher {
    Optional<SimpleExtension.ScalarFunctionVariant> tryMatch(Type type, Type outputType);
  }

  public interface SignatureMatcher {
    Optional<SimpleExtension.Function> tryMatch(List<Type> types, Type outputType);
  }

  private static SignatureMatcher chainedSignature(SignatureMatcher... matchers) {
    return switch(matchers.length) {
      case 0 -> (types, outputType) -> Optional.empty();
      case 1 -> matchers[0];
      default -> (types, outputType) -> {
        for (SignatureMatcher m : matchers) {
          var t = m.tryMatch(types, outputType);
          if (t.isPresent()) {
            return t;
          }
        }
        return Optional.empty();
      };
    };
  }

  public static class IgnoreNullableAndParameters implements TypeVisitor<Boolean, RuntimeException> {

    private final ParameterizedType typeToMatch;

    public IgnoreNullableAndParameters(ParameterizedType typeToMatch) {
      this.typeToMatch = typeToMatch;
    }

    @Override
    public Boolean visit(Type.Bool type) {
      return typeToMatch instanceof Type.Bool;
    }

    @Override
    public Boolean visit(Type.I8 type) {
      return typeToMatch instanceof Type.I8;
    }

    @Override
    public Boolean visit(Type.I16 type) {
      return typeToMatch instanceof Type.I16;
    }

    @Override
    public Boolean visit(Type.I32 type) {
      return typeToMatch instanceof Type.I32;
    }

    @Override
    public Boolean visit(Type.I64 type) {
      return typeToMatch instanceof Type.I64;
    }

    @Override
    public Boolean visit(Type.FP32 type) {
      return typeToMatch instanceof Type.FP32;
    }

    @Override
    public Boolean visit(Type.FP64 type) {
      return typeToMatch instanceof Type.FP64;
    }

    @Override
    public Boolean visit(Type.Str type) {
      return typeToMatch instanceof Type.Str;
    }

    @Override
    public Boolean visit(Type.Binary type) {
      return typeToMatch instanceof Type.Binary;
    }

    @Override
    public Boolean visit(Type.Date type) {
      return typeToMatch instanceof Type.Date;
    }

    @Override
    public Boolean visit(Type.Time type) {
      return typeToMatch instanceof Type.Time;
    }

    @Override
    public Boolean visit(Type.TimestampTZ type) {
      return typeToMatch instanceof Type.TimestampTZ;
    }

    @Override
    public Boolean visit(Type.Timestamp type) {
      return typeToMatch instanceof Type.Timestamp;
    }

    @Override
    public Boolean visit(Type.IntervalYear type) {
      return typeToMatch instanceof Type.IntervalYear;
    }

    @Override
    public Boolean visit(Type.IntervalDay type) {
      return typeToMatch instanceof Type.IntervalDay;
    }

    @Override
    public Boolean visit(Type.UUID type) {
      return typeToMatch instanceof Type.UUID;
    }

    @Override
    public Boolean visit(Type.FixedChar type) {
      return typeToMatch instanceof Type.FixedChar || typeToMatch instanceof ParameterizedType.FixedChar;
    }

    @Override
    public Boolean visit(Type.VarChar type) {
      return typeToMatch instanceof Type.VarChar || typeToMatch instanceof ParameterizedType.VarChar;
    }

    @Override
    public Boolean visit(Type.FixedBinary type) {
      return typeToMatch instanceof Type.FixedBinary || typeToMatch instanceof ParameterizedType.FixedBinary;
    }

    @Override
    public Boolean visit(Type.Decimal type) {
      return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
    }

    @Override
    public Boolean visit(Type.Struct type) {
      return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
    }

    @Override
    public Boolean visit(Type.ListType type) {
      return typeToMatch instanceof Type.ListType || typeToMatch instanceof ParameterizedType.ListType;
    }

    @Override
    public Boolean visit(Type.Map type) {
      return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
    }
  }

}
