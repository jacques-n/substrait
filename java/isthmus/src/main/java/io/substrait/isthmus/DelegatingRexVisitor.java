package io.substrait.isthmus;

import org.apache.calcite.rex.*;

import java.util.List;

public class DelegatingRexVisitor<T> implements RexVisitor<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DelegatingRexVisitor.class);

  private final DelegatingRexVisitor<T> delegate;

  public DelegatingRexVisitor(DelegatingRexVisitor<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public T visitInputRef(RexInputRef inputRef) {
    return delegate.visitInputRef(inputRef);
  }

  @Override
  public T visitLocalRef(RexLocalRef localRef) {
    return delegate.visitLocalRef(localRef);
  }

  @Override
  public T visitLiteral(RexLiteral literal) {
    return delegate.visitLiteral(literal);
  }

  @Override
  public T visitCall(RexCall call) {
    return delegate.visitCall(call);
  }

  @Override
  public T visitOver(RexOver over) {
    return delegate.visitOver(over);
  }

  @Override
  public T visitCorrelVariable(RexCorrelVariable correlVariable) {
    return delegate.visitCorrelVariable(correlVariable);
  }

  @Override
  public T visitDynamicParam(RexDynamicParam dynamicParam) {
    return delegate.visitDynamicParam(dynamicParam);
  }

  @Override
  public T visitRangeRef(RexRangeRef rangeRef) {
    return delegate.visitRangeRef(rangeRef);
  }

  @Override
  public T visitFieldAccess(RexFieldAccess fieldAccess) {
    return delegate.visitFieldAccess(fieldAccess);
  }

  @Override
  public T visitSubQuery(RexSubQuery subQuery) {
    return delegate.visitSubQuery(subQuery);
  }

  @Override
  public T visitTableInputRef(RexTableInputRef fieldRef) {
    return delegate.visitTableInputRef(fieldRef);
  }

  @Override
  public T visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return delegate.visitPatternFieldRef(fieldRef);
  }

  @Override
  public void visitList(Iterable<? extends RexNode> exprs, List<T> out) {
    delegate.visitList(exprs, out);
  }

  @Override
  public List<T> visitList(Iterable<? extends RexNode> exprs) {
    return delegate.visitList(exprs);
  }

  @Override
  public void visitEach(Iterable<? extends RexNode> exprs) {
    delegate.visitEach(exprs);
  }
}
