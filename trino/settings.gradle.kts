plugins {
    // Auto-provisions the Java 25 toolchain (Trino 481+ requires JDK 25 to build/run:
    // https://trino.io/docs/current/release/release-479.html) so the build works even
    // when the host JDK is older (e.g. Cassandra's own build commonly runs on JDK 11/17/21).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "cassandra-arrow-flight-trino-connector"
