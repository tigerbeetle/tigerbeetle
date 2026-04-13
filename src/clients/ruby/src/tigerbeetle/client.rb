module TigerBeetle
  class Client
    def self.open(cluster_id:, replica_addresses:)
      client = new(cluster_id: cluster_id, replica_addresses: replica_addresses)
      yield client
    ensure
      client&.close
    end

    def initialize(cluster_id:, replica_addresses:)
      addresses = Array(replica_addresses).join(",")
      @native = NativeClient.new(cluster_id, addresses)
    end

    def close
      @native.close
    end

    def closed?
      @native.closed?
    end

    def create_accounts(accounts)
      @native.submit(Operation::CREATE_ACCOUNTS, accounts)
    end

    def create_transfers(transfers)
      @native.submit(Operation::CREATE_TRANSFERS, transfers)
    end

    def lookup_accounts(ids)
      @native.submit(Operation::LOOKUP_ACCOUNTS, ids)
    end
  end
end
