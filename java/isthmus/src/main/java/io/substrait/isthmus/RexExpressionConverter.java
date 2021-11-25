package io.substrait.isthmus;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import org.apache.calcite.rex.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class RexExpressionConverter implements RexVisitor<Expression> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RexExpressionConverter.class);

  private static final List<CallConverter> STATIC_CONVERTERS = ImmutableList.<CallConverter>of(
      new FieldSelectionConverter(),
      new LiteralConstructorConverter(),
      CallConverters.CASE
  );

  private final List<CallConverter> callConverters;

  public RexExpressionConverter(CallConverter... callConverters) {
    this(Arrays.asList(callConverters));
  }
  
  public RexExpressionConverter(List<CallConverter> callConverters) {
    this.callConverters = callConverters;
  }

  public RexExpressionConverter() {
    this.callConverters = STATIC_CONVERTERS;
  }

  @FunctionalInterface
  public interface CallConverter {
    Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter);
  }
  
  @Override
  public Expression visitInputRef(RexInputRef inputRef) {
    return FieldReference.newRootStructReference(inputRef.getIndex(), TypeConverter.convert(inputRef.getType()));
  }

  @Override
  public Expression visitCall(RexCall call) {
    for (var c : callConverters) {
      var out = c.convert(call, r -> r.accept(this));
      if (out.isPresent()) {
        return out.get();
      }
    }

    throw new IllegalArgumentException("unable to convert call " + call);
  }

  @Override
  public Expression visitLiteral(RexLiteral literal) {
    return LiteralConverter.convert(literal);
  }

  @Override
  public Expression visitOver(RexOver over) {
    throw new UnsupportedOperationException("RexOver not supported");
  }

  @Override
  public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new UnsupportedOperationException("RexCorrelVariable not supported");
  }

  @Override
  public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new UnsupportedOperationException("RexDynamicParam not supported");
  }

  @Override
  public Expression visitRangeRef(RexRangeRef rangeRef) {
    throw new UnsupportedOperationException("RexRangeRef not supported");
  }

  @Override
  public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
    throw new UnsupportedOperationException("RexFieldAccess not supported");
  }

  @Override
  public Expression visitSubQuery(RexSubQuery subQuery) {
    throw new UnsupportedOperationException("RexSubQuery not supported");
  }

  @Override
  public Expression visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException("RexTableInputRef not supported");
  }

  @Override
  public Expression visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException("RexLocalRef not supported");
  }

  @Override
  public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new UnsupportedOperationException("RexPatternFieldRef not supported");
  }

}
