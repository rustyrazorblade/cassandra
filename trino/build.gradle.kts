plugins {
    java
}

group = "io.cassandra.trino"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        // Trino 481 requires JDK 25 to build/run (release-479 broke compatibility with
        // older JDKs); the foojay resolver in settings.gradle.kts auto-provisions it.
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

// Trino version this connector targets. 481 was the current stable release at the time
// this connector was written (see trino/README.md for how to verify/bump it).
val trinoVersion = "481"

// Pinned to 19.0.0 to match the Cassandra-side Arrow Flight PoC
// (org.apache.cassandra.arrow.*, see .build/cassandra-deps-maven-pom.xml /
// lib/arrow-vector-19.0.0.jar) so client and server negotiate the same Arrow IPC
// format. flight-core is the only Arrow artifact declared directly; arrow-vector,
// arrow-memory-*, and arrow-format ride in transitively at this same version.
val arrowVersion = "19.0.0"

// arrow-java 19's off-heap allocator (NettyAllocationManager) is broken under Netty
// 4.2.x: UnsafeDirectLittleEndian.<init> calls EmptyByteBuf.memoryAddress(), which
// Netty 4.2 changed to throw UnsupportedOperationException, failing static init on the
// very first RootAllocator. flight-core:19.0.0's own transitive Netty requests land on
// 4.1.130.Final via Maven's nearest-wins, but Gradle's module-metadata resolution can
// float several core Netty modules up to a newer 4.2.x line. Pin the whole Netty stack
// down to the version arrow-java 19 was actually built/tested against.
val nettyVersion = "4.1.130.Final"
val nettyCoreModules = listOf(
    "netty-buffer",
    "netty-codec",
    "netty-codec-http",
    "netty-codec-http2",
    "netty-codec-socks",
    "netty-common",
    "netty-handler",
    "netty-handler-proxy",
    "netty-resolver",
    "netty-transport",
    "netty-transport-native-unix-common",
)

dependencies {
    // Provided by the Trino engine's own classpath at runtime; bundling it would clash
    // with the engine's copy. This is the standard Trino plugin convention.
    compileOnly("io.trino:trino-spi:$trinoVersion")

    implementation(platform("io.netty:netty-bom:$nettyVersion"))
    nettyCoreModules.forEach {
        implementation("io.netty:$it") { version { strictly(nettyVersion) } }
    }
    implementation("org.apache.arrow:flight-core:$arrowVersion")

    // Used to build/serialize the Flight ticket JSON (tokenRange/filter/aggregation - see
    // ARROW-FLIGHT.md and ArrowFlightTicket). Already present transitively (arrow-vector and
    // sidecar-client both pull it in), declared explicitly so the version isn't left to whatever
    // those other libraries happen to request.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.0")

    // Sidecar ring/topology discovery. sidecar-client is transport-agnostic (ships only the
    // HttpClient interface); sidecar-vertx-client supplies the actual Vert.x-based HttpClient
    // implementation (VertxHttpClient/VertxRequestExecutor) - without it SidecarClient has no
    // concrete way to make an HTTP call. Both confirmed present on Maven Central as separate
    // artifacts (there is no fat/shaded "sidecar-client-all" published for plain consumption;
    // cassandra-analytics' own Sidecar.java wiring - read from cassandra-analytics-sidecar-client's
    // sources jar - was used as the template for how these two are wired together, since
    // sidecar-client's own javadoc/sources jars are not published).
    implementation("org.apache.cassandra:sidecar-client:0.4.0")
    implementation("org.apache.cassandra:sidecar-vertx-client:0.4.0")

    // Ring/split math (CassandraRing, TokenPartitioner, RangeUtils) - confirmed to have zero
    // Spark dependencies. Note: despite CassandraRing/TokenPartitioner/RangeUtils importing
    // com.google.common.collect.Range/RangeMap/etc. at the source level, this artifact declares
    // NO Guava dependency of its own (verified: its Gradle module metadata lists only slf4j-api;
    // Guava is not shaded into the jar either). It compiles/links today only because Guava
    // happens to ride in transitively from flight-core's grpc-netty dependency below - which is
    // an accident of the current dependency graph, not a guarantee. Pin Guava explicitly so this
    // keeps working even if that transitive path ever changes; -jre (not -android) since this is
    // a plain JVM Trino plugin.
    implementation("org.apache.cassandra:cassandra-analytics-common:0.4.0")
    implementation("com.google.guava:guava:33.5.0-jre")

    // cassandra-analytics-common's own Serializable classes (CassandraRing, TokenPartitioner,
    // CassandraInstance, ReplicationFactor) each declare a
    // "public static final Serializer SERIALIZER = new Serializer()" field whose nested
    // Serializer extends com.esotericsoftware.kryo.Serializer - so merely *loading* any of those
    // classes (not calling anything Kryo-specific) eagerly constructs that nested class in a
    // static initializer, which fails with NoClassDefFoundError without Kryo on the classpath.
    // Confirmed at test time (ReplicationFactor.<clinit> failing under SplitPlannerTest); not
    // declared as a dependency of cassandra-analytics-common's own POM/module metadata at all
    // (Spark's bulk-reader environment normally supplies its own Kryo), so it must be added here
    // explicitly even though this connector never uses Kryo serialization itself.
    implementation("com.esotericsoftware:kryo:5.6.2")

    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    // Arrow's off-heap allocator needs the foreign-memory module opened, and (on newer
    // JDKs) native access enabled, or RootAllocator construction fails.
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
}

// Assembles the plugin directory (this connector's jar + every runtime dependency) the
// way Trino expects: one flat directory of jars under plugin/<connector-name>/, loaded
// through its own isolated classloader. See trino/README.md for install steps.
tasks.register<Sync>("installPlugin") {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("plugin/cassandra_arrow_flight"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}
