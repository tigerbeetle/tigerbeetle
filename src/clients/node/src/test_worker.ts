import { createClient } from ".";
import { workerData, parentPort } from "worker_threads";
const { cluster_id, replica_addresses } = workerData;
async function main() {
  const client = createClient({
      cluster_id,
      replica_addresses,
  });
  await client.queryAccounts({
      limit: 9000,
      ledger: 1,
      user_data_128: 0n,
      code: 0,
      flags: 0,
      timestamp_max: 0n,
      timestamp_min: 0n,
      user_data_32: 0,
      user_data_64: 0n,
  });
    parentPort?.postMessage('done');
}

main()
