package io.substrait.isthmus;

import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

public class PlanEntryPoint implements Callable<Integer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanEntryPoint.class);

  @CommandLine.Parameters(index = "0", description = "The sql we should parse.")
  private String sql;

  @CommandLine.Option(names = {"-c", "--create"}, description = "Create table statements e.g. CREATE TABLE T1(foo int, bar bigint)")
  private List<String> createStatements;

  @Override
  public Integer call() throws Exception {
    SqlToSubstrait converter = new SqlToSubstrait();
    converter.execute(sql, createStatements);
    return 0;
  }

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    int exitCode = new CommandLine(new PlanEntryPoint()).execute(args);
    System.exit(exitCode);
  }
}
