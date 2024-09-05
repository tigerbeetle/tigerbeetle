FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y --no-install-recommends wget ca-certificates

RUN wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb && dpkg -i jdk-21_linux-x64_bin.deb && rm jdk-21_linux-x64_bin.deb

COPY src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar ./tigerbeetle-java-0.0.1-SNAPSHOT.jar
COPY src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar ./workload-0.0.1-SNAPSHOT.jar

ENTRYPOINT ["java", "-ea", "-cp", "workload-0.0.1-SNAPSHOT.jar:tigerbeetle-java-0.0.1-SNAPSHOT.jar", "Main"]
