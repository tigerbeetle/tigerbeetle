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
      @closed = false
    end

    def close
      raise ClientClosedError, "client is already closed" if closed?

      @closed = true
      @native.close
    end

    def closed?
      @closed
    end

    def create_accounts(accounts) = native_submit(Operation::CREATE_ACCOUNTS, accounts)
    def create_transfers(transfers) = native_submit(Operation::CREATE_TRANSFERS, transfers)
    def lookup_accounts(ids) = native_submit(Operation::LOOKUP_ACCOUNTS, ids)
    def lookup_transfers(ids) = native_submit(Operation::LOOKUP_TRANSFERS, ids)

    private

    def native_submit(operation, payload)
      return [] if payload.empty?

      read_io, write_io = IO.pipe
      req = @native.submit(operation, payload, write_io.fileno)
      read_io.read(1)
      status, result = req.result
      raise ClientClosedError if status == PACKET_CLIENT_SHUTDOWN
      raise PacketError, status unless status == PACKET_OK

      result
    ensure
      read_io&.close
      write_io&.close
    end
  end
end
