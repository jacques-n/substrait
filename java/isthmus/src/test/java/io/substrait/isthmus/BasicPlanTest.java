package io.substrait.isthmus;

import io.substrait.type.Type;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import io.substrait.proto.Expression;
import io.substrait.proto.Rel;
import io.substrait.function.TypeExpression;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("Need to rework with abstraction layer.")
public class BasicPlanTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicPlanTest.class);

  private final SubstraitRelVisitor visitor = new SubstraitRelVisitor();
  private final RelCreator creator = new RelCreator();
  private final RelBuilder builder = creator.createRelBuilder();
  private final RexBuilder rex = creator.rex();
  private final RelDataTypeFactory type = creator.type();

  @Test
  public void roundtripRowType() {
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
  }

  @Test
  void literalInteger() {
    testLiteral(Expression.Literal.newBuilder().setI32(1), "1");
  }

  @Test
  void integerType() {
    testType(Type.REQUIRED.I8, SqlTypeName.TINYINT);
  }
  @Test
  void bigint() {
    testLiteral(Expression.Literal.newBuilder().setI64(1), "CAST(1 as BIGINT)");
//    rex.makeLiteral(1, RelData)
//    testReverseLiteral()
  }

  private void testType(TypeExpression expression, SqlTypeName type) {
    testType(expression, this.type.createSqlType(type));
  }

  private void testType(TypeExpression expression, RelDataType type) {
    assertEquals(expression, TypeConverter.convert(type));
  }

  private void testLiteral(Expression.Literal.Builder expectedLiteral, String sqlSubExpr) {
    RelCreator c = new RelCreator();
    RelRoot root = c.parse(String.format("(VALUES %s)", sqlSubExpr));
    Rel rel = new SubstraitRelVisitor().reverseAccept(root.rel);
    assertEquals(expectedLiteral.build(),
        rel.getRead().getVirtualTable().getValues(0).getFields(0));
  }

  private void literal(Expression.Literal.Builder substraitLiteral, RexNode calciteExpression) {
    expression(Expression.newBuilder().setLiteral(substraitLiteral), calciteExpression);
  }
  
  private void expression(Expression.Builder substraitExpression, RexNode calciteExpression) {
    assertEquals(substraitExpression.build(), calciteExpression.accept(new RexExpressionConverter()));
  }

}
