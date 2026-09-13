FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 10001 kv && mkdir -p /data && chown kv /data
COPY --from=build /src/target/distribukv.jar /app/distribukv.jar
USER kv
ENV KV_DATA_DIR=/data \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/distribukv.jar"]
