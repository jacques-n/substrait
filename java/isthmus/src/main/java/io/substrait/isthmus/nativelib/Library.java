package io.substrait.isthmus.nativelib;

import io.substrait.isthmus.SqlToSubstrait;
import org.apache.calcite.sql.parser.SqlParseException;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.IsolateThread;

import java.util.List;

public final class Library {

    // This run is useful for "gradle -Pagent run" which will automatically generate
    // GraalVM native image config based on access, via instrumentation
    public static void main(String[] args) throws SqlParseException {
        List<String> tables = List.of("CREATE TABLE person (id int, name varchar, age int)");
        String query = "SELECT id, name, age FROM person WHERE age > 20";
        SqlToSubstrait.execute(query, tables);
    }

    // NOTE: This only handles a single table atm
    @CEntryPoint(name = "getCalcitePlan")
    public static CCharPointer getCalcitePlan(IsolateThread thread, CCharPointer sql, CCharPointer tables)
            throws SqlParseException {
        // Convert C *char to Java String
        final String sqlString = CTypeConversion.toJavaString(sql);
        final String tablesString = CTypeConversion.toJavaString(tables);

        // logic goes here
        String result = SqlToSubstrait.execute(sqlString, List.of(tablesString));

        // Convert Java String to C *char
        try (final CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(result)) {
            final CCharPointer resultCStr = holder.get();
            return resultCStr;
        }
    }

}
