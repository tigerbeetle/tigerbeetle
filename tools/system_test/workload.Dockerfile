FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends  \
    openjdk-17-jre

COPY src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar ./tigerbeetle-java-0.0.1-SNAPSHOT.jar
COPY src/testing/system_test/workload/target/workload-0.0.1-SNAPSHOT.jar ./workload-0.0.1-SNAPSHOT.jar

ENTRYPOINT ["java", "-cp", "workload-0.0.1-SNAPSHOT.jar:tigerbeetle-java-0.0.1-SNAPSHOT.jar", "Main"]
