package io.substrait.isthmus;

import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.List;

public class SubstraitRelVisitor extends RelVisitor<Rel, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SubstraitRelVisitor.class);


  private static RelCommon direct() {
    return RelCommon.newBuilder().setDirect(RelCommon.Direct.newBuilder()).build();
  }

  private static RelCommon reorder(List<Integer> integers) {
    return RelCommon.newBuilder().setEmit(RelCommon.Emit.newBuilder().addAllOutputMapping(integers)).build();
  }

  @Override
  public Rel visit(TableScan scan) {
    return Rel.newBuilder().setRead(ReadRel.newBuilder()
        .setCommon(direct())
        .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(scan.getTable().getQualifiedName()))
    ).build();
  }

  @Override
  public Rel visit(TableFunctionScan scan) {
    return super.visit(scan);
  }

  @Override
  public Rel visit(LogicalValues values) {
//    List<Expression.Literal.Struct> structs = values.getTuples().stream().map(list -> {
//      var fields =  list.stream().map(l -> LiteralConverter.convert(l)).collect(Collectors.toUnmodifiableList());
//      return Expression.Literal.Struct.newBuilder().addAllFields(fields).build();
//    }).collect(Collectors.toUnmodifiableList());
//    return Rel.newBuilder().setRead(ReadRel.newBuilder().setCommon(direct()).setVirtualTable(ReadRel.VirtualTable.newBuilder().addAllValues(structs))).build();
    return null;
  }

  @Override
  public Rel visit(LogicalFilter filter) {
    return super.visit(filter);
  }

  @Override
  public Rel visit(LogicalCalc calc) {
    return super.visit(calc);
  }

  @Override
  public Rel visit(LogicalProject project) {
    Rel input = reverseAccept(project.getInput());
    var expressions = project.getProjects().stream().map(e -> e.accept(new RexExpressionConverter())).toList();

    // todo: eliminate excessive projects.
    //return Rel.newBuilder().setProject(ProjectRel.newBuilder().setCommon(direct()).addAllExpressions(expressions).setInput(input)).build();
    return null;
  }

  @Override
  public Rel visit(LogicalJoin join) {
    return null;
//    Rel left = reverseAccept(join.getLeft());
//    Rel right = reverseAccept(join.getRight());
//    var condition = join.getCondition().accept(new SubstraitExpressionVisitor());
//    return Rel.newBuilder().setJoin(
//        JoinRel.newBuilder().setCommon(direct()).setLeft(left).setRight(right).setExpression(condition)).build();
  }

  @Override
  public Rel visit(LogicalCorrelate correlate) {
    return super.visit(correlate);
  }

  @Override
  public Rel visit(LogicalUnion union) {
    return super.visit(union);
  }

  @Override
  public Rel visit(LogicalIntersect intersect) {
    return super.visit(intersect);
  }

  @Override
  public Rel visit(LogicalMinus minus) {
    return super.visit(minus);
  }

  @Override
  public Rel visit(LogicalAggregate aggregate) {
    return super.visit(aggregate);
  }

  @Override
  public Rel visit(LogicalMatch match) {
    return super.visit(match);
  }

  @Override
  public Rel visit(LogicalSort sort) {
    return super.visit(sort);
  }

  @Override
  public Rel visit(LogicalExchange exchange) {
    return super.visit(exchange);
  }

  @Override
  public Rel visit(LogicalTableModify modify) {
    return super.visit(modify);
  }

  @Override
  public Rel visitOther(RelNode other) {
    throw new UnsupportedOperationException("Unable to handle node: " + other);

  }

  public static Rel apply(RelNode r) {
    SubstraitRelVisitor v = new SubstraitRelVisitor();
    return v.reverseAccept(r);
  }
}
