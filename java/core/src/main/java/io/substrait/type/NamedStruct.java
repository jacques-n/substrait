package io.substrait.type;


import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
public interface NamedStruct {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamedStruct.class);

  Type.Struct struct();
  List<String> names();
}
