# Builds the Arrow Flight Trino connector plugin (see ../build.gradle.kts) and layers it into
# the official Trino image. Build context is trino/ (this file's parent directory) - see
# ../docker-compose.yml.
#
# JDK 25 matches this connector's Gradle toolchain requirement (Trino 481 needs JDK 25 to
# build/run - see ../build.gradle.kts) - using it directly as the build stage's own JDK avoids
# Gradle's foojay auto-provisioner needing network access to fetch a toolchain.
FROM eclipse-temurin:25-jdk-jammy AS build
WORKDIR /connector
COPY . .
RUN ./gradlew --no-daemon installPlugin

# Catalog config (arrow_flight.properties) is bind-mounted by docker-compose.yml rather than
# baked in here, so changing the target host/port doesn't require rebuilding this image.
FROM trinodb/trino:481
COPY --from=build /connector/build/plugin/cassandra_arrow_flight /usr/lib/trino/plugin/cassandra_arrow_flight
