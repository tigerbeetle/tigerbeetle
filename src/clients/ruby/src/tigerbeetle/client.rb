module TigerBeetle
  # TigerBeetle client.
  #
  # A client can be shared by concurrent threads and fibers for request methods.
  # Public request methods are synchronous and return after the request
  # completes. When a fiber scheduler is active, waiting for a response yields
  # to the scheduler.
  #
  # Prefer {.open} for lifecycle management so the client is closed once after
  # concurrent work completes. Instantiate multiple clients when connecting to
  # more than one TigerBeetle cluster.
  class Client
    Client::COMPLETION_DISPATCHER = CompletionDispatcher.new
    private_constant :COMPLETION_DISPATCHER

    # Opens a client for the duration of the block and closes it before
    # returning.
    #
    # Yields the open client and returns the block return value.
    def self.open(cluster_id:, replica_addresses:)
      client = new(cluster_id: cluster_id, replica_addresses: replica_addresses)
      yield client
    ensure
      client&.close
    end

    # Initializes a TigerBeetle client.
    #
    # @raise [TigerBeetle::InitError] if the native client cannot be
    #   initialized.
    def initialize(cluster_id:, replica_addresses:)
      addresses = Array(replica_addresses).join(",")
      @native = NativeClient.new(cluster_id, addresses, COMPLETION_DISPATCHER.write_fileno)
      @closed = false
    end

    # Closes the client. This method waits for all in-flight requests to finish.
    #
    # @raise [TigerBeetle::ClientClosedError] if the client is already closed.
    def close
      raise ClientClosedError, "client is already closed" if closed?

      @closed = true
      @native.close
    end

    # Returns whether the client has been closed.
    def closed?
      @closed
    end

    # Submits a batch of new accounts to be created.
    #
    # The returned array contains one {TigerBeetle::CreateAccountResult} for
    # each submitted account.
    #
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def create_accounts(accounts) = native_submit(Operation::CREATE_ACCOUNTS, accounts)

    # Submits a batch of new transfers to be created.
    #
    # The returned array contains one {TigerBeetle::CreateTransferResult} for
    # each submitted transfer.
    #
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def create_transfers(transfers) = native_submit(Operation::CREATE_TRANSFERS, transfers)

    # Looks up a batch of accounts.
    #
    # The returned array contains all accounts found. Accounts not found are
    # omitted.
    #
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def lookup_accounts(ids) = native_submit(Operation::LOOKUP_ACCOUNTS, ids)

    # Looks up a batch of transfers.
    #
    # The returned array contains all transfers found. Transfers not found are
    # omitted.
    #
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def lookup_transfers(ids) = native_submit(Operation::LOOKUP_TRANSFERS, ids)

    # Fetches transfers from a given account.
    #
    # Returns transfers that match the query parameters.
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def get_account_transfers(filter) = native_submit(Operation::GET_ACCOUNT_TRANSFERS, [filter])

    # Fetches balance history from a given account.
    #
    # Returns balances that match the query parameters.
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def get_account_balances(filter) = native_submit(Operation::GET_ACCOUNT_BALANCES, [filter])

    # Queries accounts.
    #
    # Returns accounts that match the query parameters.
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def query_accounts(filter) = native_submit(Operation::QUERY_ACCOUNTS, [filter])

    # Queries transfers.
    #
    # Returns transfers that match the query parameters.
    # @raise [TigerBeetle::PacketError] if the entire request fails.
    # @raise [TigerBeetle::ClientClosedError] if the client is closed.
    def query_transfers(filter) = native_submit(Operation::QUERY_TRANSFERS, [filter])

    private

    def native_submit(operation, payload)
      raise ClientClosedError if closed?

      req = COMPLETION_DISPATCHER.submit_and_wait_for(@native, operation, payload)

      status, result = req.result
      raise ClientClosedError if status == PACKET_CLIENT_SHUTDOWN
      raise PacketError, status unless status == PACKET_OK

      result
    end
  end
end
