import java.security.SecureRandom;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import com.tigerbeetle.Client;
import com.tigerbeetle.UInt128;

public final class Main {
  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    String replicaAddressesArg = Objects.requireNonNull(env.get("REPLICAS"),
        "REPLICAS environment variable must be set (comma-separated list)");

    String[] replicaAddresses = replicaAddressesArg.split(",");
    if (replicaAddresses.length == 0) {
      throw new IllegalArgumentException(
          "REPLICAS must list at least one address (comma-separated)");
    }

    Random random = new SecureRandom();
    byte[] clusterID = UInt128.asBytes(Long.parseLong(env.getOrDefault("CLUSTER", "1")));

    try (var client = new Client(clusterID, replicaAddresses)) {
      new Workload(random, client).run();
    }
  }
}
