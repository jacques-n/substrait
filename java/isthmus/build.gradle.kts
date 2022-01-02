plugins {
    id("java")
    id("application")
    id("idea")
    id("com.palantir.graal") version "0.10.0"
    id("org.graalvm.buildtools.native") version "0.9.9"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("io.substrait.isthmus.nativelib.Library")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("--enable-preview")
}

tasks.withType<Test> {
    jvmArgs("--enable-preview")
}

tasks.withType<JavaExec> {
    jvmArgs("--enable-preview")
}


dependencies {
    implementation(project(":core"))
    implementation("org.apache.calcite:calcite-core:1.28.0")
    implementation("org.apache.calcite:calcite-server:1.28.0")
    implementation("org.junit.jupiter:junit-jupiter:5.7.0")
    implementation("org.reflections:reflections:0.9.12")
    implementation("org.graalvm.sdk:graal-sdk:21.2.0")
    implementation("info.picocli:picocli:4.6.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.4")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.4")
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("com.github.ben-manes.caffeine:caffeine:3.0.4")
}


// Blindly copied this from Jacques
val initializeAtBuildTime = listOf(
    "org.slf4j.LoggerFactory",
    "org.slf4j.impl.StaticLoggerBinder",
    "org.slf4j.impl.JDK14LoggerAdapter",
    "org.apache.commons.codec.language.Soundex",
    "org.apache.calcite.util.Util",
    "org.apache.calcite.util.Pair",
    "org.apache.calcite.rel.metadata.RelMdUniqueKeys",
    "org.apache.calcite.rel.metadata.RelMdTableReferences",
    "org.apache.calcite.rel.metadata.RelMdSize",
    "org.apache.calcite.rel.metadata.RelMdSelectivity",
    "org.apache.calcite.rel.metadata.RelMdRowCount",
    "org.apache.calcite.rel.metadata.RelMdPredicates",
    "org.apache.calcite.rel.metadata.RelMdPopulationSize",
    "org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows",
    "org.apache.calcite.rel.metadata.RelMdParallelism",
    "org.apache.calcite.rel.metadata.RelMdNodeTypes",
    "org.apache.calcite.rel.metadata.RelMdMinRowCount",
    "org.apache.calcite.rel.metadata.RelMdMemory",
    "org.apache.calcite.rel.metadata.RelMdMaxRowCount",
    "org.apache.calcite.rel.metadata.RelMdLowerBoundCost",
    "org.apache.calcite.rel.metadata.RelMdExpressionLineage",
    "org.apache.calcite.rel.metadata.RelMdExplainVisibility",
    "org.apache.calcite.rel.metadata.RelMdDistribution",
    "org.apache.calcite.rel.metadata.RelMdDistinctRowCount",
    "org.apache.calcite.rel.metadata.RelMdColumnUniqueness",
    "org.apache.calcite.rel.metadata.RelMdColumnOrigins",
    "org.apache.calcite.rel.metadata.RelMdCollation",
    "org.apache.calcite.rel.metadata.RelMdAllPredicates",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$UniqueKeys",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$TableReferences",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Size",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Selectivity",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$RowCount",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Predicates",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$PopulationSize",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$PercentageOriginalRows",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Parallelism",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$NonCumulativeCost",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$NodeTypes",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$MinRowCount",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Memory",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$MaxRowCount",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$LowerBoundCost",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$ExpressionLineage",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$ExplainVisibility",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Distribution",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$DistinctRowCount",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$CumulativeCost",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnUniqueness",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnOrigin",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$Collation",
    "org.apache.calcite.rel.metadata.BuiltInMetadata\$AllPredicates",
    "org.apache.calcite.config.CalciteSystemProperty",
    "com.google.common.util.concurrent.SettableFuture",
    "com.google.common.util.concurrent.AbstractFuture\$UnsafeAtomicHelper",
    "com.google.common.util.concurrent.AbstractFuture",
    "com.google.common.util.concurrent.AbstractFuture",
    "com.google.common.primitives.Primitives",
    "com.google.common.math.IntMath\$1",
    "com.google.common.math.IntMath",
    "com.google.common.collect.RegularImmutableSortedSet",
    "com.google.common.collect.RegularImmutableMap",
    "com.google.common.collect.Range",
    "com.google.common.collect.Platform",
    "com.google.common.collect.ImmutableSortedMap",
    "com.google.common.collect.ImmutableRangeSet",
    "com.google.common.cache.LocalCache",
    "com.google.common.cache.CacheBuilder",
    "com.google.common.base.Preconditions",
    "com.google.common.base.Platform",
    "io.substrait.isthmus.metadata.LegacyToLambdaGenerator",
    "io.substrait.isthmus.metadata.LambdaMetadataSupplier",
    "io.substrait.isthmus.metadata.LambdaHandlerCache",
)

graalvmNative {
    binaries.configureEach {
        agent {
            options.add("experimental-class-loader-support")
        }
    }

    binaries {
        named("main") {
            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(17))
                vendor.set(JvmVendorSpec.matching("GraalVM Community"))
            })

            // Main options
            imageName.set("libisthmus") // The name of the native image, defaults to the project name
            mainClass.set("io.substrait.isthmus.nativelib.Library") // The main class to use, defaults to the application.mainClass
            debug.set(true) // Determines if debug info should be generated, defaults to false
            verbose.set(true) // Add verbose output, defaults to false
            fallback.set(true) // Sets the fallback mode of native-image, defaults to false
            sharedLibrary.set(true) // Determines if image is a shared library, defaults to false if `java-library` plugin isn't included

            // systemProperties.putAll(mapOf("name1" to "value1", "name2" to "value2")) // Sets the system properties to use for the native image builder
            // configurationFileDirectories.from(file("src/my-config")) // Adds a native image configuration file directory, containing files like reflection configuration

            // Advanced options
            buildArgs.add("--initialize-at-build-time=" + initializeAtBuildTime.joinToString(",")) // Passes '-H:Extra' to the native image builder options. This can be used to pass parameters which are not directly supported by this extension
            // jvmArgs.add("flag") // Passes 'flag' directly to the JVM running the native image builder

            // Runtime options
            // runtimeArgs.add("--help") // Passes '--help' to built image, during "nativeRun" task

            // This causes error during native build if set to true, but without it Windows fails with "The command is too long"
            // This makes it impossible to build on Windows unfortunately
            // "error: java.lang.SecurityException: Invalid signature file digest for Manifest main attributes"
            useFatJar.set(true)

            // Development options
            // agent {
            //    enabled.set(true) // Enables the reflection agent. Can be also set on command line using '-Pagent'
            // }
        }
    }
}

tasks.withType<org.gradle.jvm.tasks.Jar>() {
    exclude("META-INF/*.RSA", "META-INF/*.DSA", "META-INF/*.SF")
}

graal {
    mainClass("io.substrait.isthmus.PlanEntryPoint")
    outputName("isthmus")
    graalVersion("21.3.0")
    javaVersion("17")
    option("--no-fallback")
    //option("--initialize-at-build-time=io.substrait.isthmus.InitializeAtBuildTime,org.slf4j.impl.StaticLoggerBinder,com.google.common.math.IntMath$1,com.google.common.base.Platform,com.google.common.util.concurrent.AbstractFuture\$UnsafeAtomicHelper,com.google.common.collect.ImmutableSortedMap,com.google.common.math.IntMath,com.google.common.collect.RegularImmutableSortedSet,com.google.common.cache.LocalCache,com.google.common.collect.Range,org.apache.commons.codec.language.Soundex,com.google.common.collect.ImmutableRangeSet,org.slf4j.LoggerFactory,com.google.common.collect.Platform,com.google.common.util.concurrent.SettableFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.cache.CacheBuilder,com.google.common.base.Preconditions,com.google.common.collect.RegularImmutableMap,org.slf4j.impl.JDK14LoggerAdapter")
    option("--initialize-at-build-time=io.substrait.isthmus.InitializeAtBuildTime,org.slf4j.impl.StaticLoggerBinder,com.google.common.math.IntMath\$1,com.google.common.base.Platform,com.google.common.util.concurrent.AbstractFuture\$UnsafeAtomicHelper,com.google.common.collect.ImmutableSortedMap,com.google.common.math.IntMath,com.google.common.collect.RegularImmutableSortedSet,com.google.common.cache.LocalCache,com.google.common.collect.Range,org.apache.commons.codec.language.Soundex,com.google.common.collect.ImmutableRangeSet,org.slf4j.LoggerFactory,com.google.common.collect.Platform,com.google.common.util.concurrent.SettableFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.cache.CacheBuilder,com.google.common.base.Preconditions,com.google.common.collect.RegularImmutableMap,org.slf4j.impl.JDK14LoggerAdapter,org.apache.calcite.rel.metadata.RelMdColumnUniqueness,org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnOrigin,io.substrait.isthmus.metadata.LambdaMetadataSupplier,org.apache.calcite.rel.metadata.BuiltInMetadata\$PopulationSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$Size,org.apache.calcite.rel.metadata.BuiltInMetadata\$UniqueKeys,org.apache.calcite.rel.metadata.RelMdColumnOrigins,org.apache.calcite.rel.metadata.RelMdExplainVisibility,org.apache.calcite.rel.metadata.RelMdMemory,org.apache.calcite.rel.metadata.RelMdExpressionLineage,org.apache.calcite.rel.metadata.RelMdDistinctRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$RowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$PercentageOriginalRows,org.apache.calcite.util.Pair,org.apache.calcite.rel.metadata.BuiltInMetadata\$ExpressionLineage,org.apache.calcite.rel.metadata.BuiltInMetadata\$MinRowCount,com.google.common.primitives.Primitives,org.apache.calcite.rel.metadata.BuiltInMetadata\$Selectivity,org.apache.calcite.rel.metadata.BuiltInMetadata\$Parallelism,org.apache.calcite.rel.metadata.RelMdUniqueKeys,org.apache.calcite.rel.metadata.RelMdParallelism,org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows,org.apache.calcite.rel.metadata.BuiltInMetadata\$Predicates,org.apache.calcite.rel.metadata.BuiltInMetadata\$Distribution,org.apache.calcite.config.CalciteSystemProperty,org.apache.calcite.rel.metadata.BuiltInMetadata\$NonCumulativeCost,org.apache.calcite.util.Util,org.apache.calcite.rel.metadata.RelMdAllPredicates,io.substrait.isthmus.metadata.LambdaHandlerCache,org.apache.calcite.rel.metadata.BuiltInMetadata\$TableReferences,org.apache.calcite.rel.metadata.RelMdNodeTypes,org.apache.calcite.rel.metadata.RelMdCollation,org.apache.calcite.rel.metadata.RelMdSelectivity,org.apache.calcite.rel.metadata.BuiltInMetadata\$NodeTypes,org.apache.calcite.rel.metadata.RelMdPredicates,org.apache.calcite.rel.metadata.BuiltInMetadata\$DistinctRowCount,org.apache.calcite.rel.metadata.RelMdRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$MaxRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$AllPredicates,org.apache.calcite.rel.metadata.RelMdMaxRowCount,org.apache.calcite.rel.metadata.RelMdLowerBoundCost,org.apache.calcite.rel.metadata.BuiltInMetadata\$ExplainVisibility,org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnUniqueness,org.apache.calcite.rel.metadata.RelMdPopulationSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$Memory,org.apache.calcite.rel.metadata.RelMdMinRowCount,org.apache.calcite.rel.metadata.RelMdSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$LowerBoundCost,org.apache.calcite.rel.metadata.RelMdTableReferences,org.apache.calcite.rel.metadata.RelMdDistribution,io.substrait.isthmus.metadata.LegacyToLambdaGenerator,org.apache.calcite.rel.metadata.BuiltInMetadata\$CumulativeCost,org.apache.calcite.rel.metadata.BuiltInMetadata\$Collation")
    //option("--initialize-at-build-time=")
    //option("--trace-class-initialization=com.fasterxml.jackson.databind.ObjectMapper")


    option("--report-unsupported-elements-at-runtime")
    option("-H:+ReportExceptionStackTraces")
    option("-H:DynamicProxyConfigurationFiles=proxies.json")
    option("--features=io.substrait.isthmus.RegisterAtRuntime")
    option("-J--enable-preview")
}
