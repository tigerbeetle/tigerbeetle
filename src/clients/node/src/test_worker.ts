import assert from "assert";
import { createClient, ErrorCodes, RequestError } from ".";
import { workerData } from "worker_threads";
const { cluster_id, replica_addresses } = workerData;
async function main() {
  const client = createClient({
    cluster_id,
    replica_addresses,
  });
  try {
    await assert.rejects(client.queryAccounts({
      limit: 9000, // Intentionally trigger a batch size error
      ledger: 1,
      user_data_128: 0n,
      code: 0,
      flags: 0,
      timestamp_max: 0n,
      timestamp_min: 0n,
      user_data_32: 0,
      user_data_64: 0n,
    }), (err: unknown) => {
        assert(err instanceof RequestError)
        assert.strictEqual(err.code, ErrorCodes.ERR_TOO_MUCH_DATA)
        return true
    });
  } finally {
    client.destroy();
  }
}

main();
