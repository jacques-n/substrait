package io.substrait.relation;

import io.substrait.type.Type;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

public abstract class Rel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Rel.class);

  private Optional<Type.Struct> recordType = Optional.empty();
  
  public abstract Optional<Remap> getRemap();

  public abstract Type.Struct deriveRecordType();
  
  public final Type.Struct getRecordType() {
    if (recordType.isEmpty()) {
      recordType = Optional.of(deriveRecordType());
    }

    return recordType.get();
  }

  public abstract List<Rel> getInputs();

  @Value.Immutable
  public static abstract class Remap {
    public abstract List<Integer> indices();

    public Type.Struct remap(Type.Struct initial) {
      return null;
    }
  }
}
