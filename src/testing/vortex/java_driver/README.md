# Vortex Java Driver

This implements a driver for Vortex, using the Java client.

Run the following to test with this driver:

```
./zig/zig build clients:java
(cd src/clients/java && mvn package)
(cd src/testing/vortex/java_driver && mvn package)
CLASS_PATH="src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar"
CLASS_PATH="${CLASS_PATH}:src/testing/vortex/java_driver/target/vortex-driver-java-0.0.1-SNAPSHOT.jar"
    zig build vortex -- supervisor --driver-command=java\ -cp\ $CLASS_PATH\ Main
```
