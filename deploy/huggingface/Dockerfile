# All-in-one image for a Hugging Face Docker Space (or any single host):
# 5 DistribuKV nodes + Kafka + MySQL + Redis in one container. node1 serves the dashboard on 7860.
# Build from the repository root:  docker build -f deploy/huggingface/Dockerfile .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY docs ./docs
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM apache/kafka:3.9.1 AS kafka

FROM eclipse-temurin:21-jre
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends mysql-server redis-server curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=kafka /opt/kafka /opt/kafka
COPY --from=build /src/target/distribukv.jar /app/distribukv.jar
COPY deploy/huggingface/start.sh /app/start.sh
# Hugging Face runs containers as UID 1000.
RUN (userdel -r ubuntu 2>/dev/null || true) \
 && useradd -m -u 1000 user \
 && chmod 755 /app/start.sh
USER user
ENV HOME=/home/user PORT=7860
EXPOSE 7860
CMD ["/app/start.sh"]
