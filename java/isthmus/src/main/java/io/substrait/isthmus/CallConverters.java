package io.substrait.isthmus;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;

public class CallConverters {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CallConverters.class);

  /**
   *
   */
  public static SimpleCallConverter CASE = (call, visitor) -> {
    if (call.getKind() != SqlKind.CASE) {
      return null;
    }

    // number of arguments are always going to be odd (each condition/then combination plus else)
    assert call.getOperands().size() % 2 == 1;

    var caseArgs = call.getOperands().stream().map(visitor).toList();

    var last = caseArgs.size() - 1;
    // for if/else, process in reverse to maintain query order
    var caseConditions = new ArrayList<Expression.IfClause>();
    for (int i = 0; i < last; i += 2) {
      caseConditions.add(ImmutableExpression.IfClause.builder().condition(caseArgs.get(i)).then(caseArgs.get(i+1)).build());
    }

    var defaultResult = caseArgs.get(last);
    return ExpressionCreator.ifThenStatement(defaultResult, caseConditions);
  };


  public interface SimpleCallConverter extends RexExpressionConverter.CallConverter {

    @Nullable Expression apply(RexCall call, Function<RexNode, Expression> topLevelConverter);
    
    @Override
    default Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
      return Optional.ofNullable(apply(call, topLevelConverter));
    }
  }
}