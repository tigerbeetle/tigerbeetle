require "minitest/autorun"
require "tigerbeetle"

class TestClientLifecycle < Minitest::Test
  def setup
    @tb_address = ENV.fetch("TB_ADDRESS", "3000")
  end

  def test_double_close
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    client.close
    client.close
  end

  def test_open_closes_after_block
    client = nil
    TigerBeetle::Client.open(cluster_id: 0, replica_addresses: @tb_address) do |c|
      client = c
    end

    assert_raises(TigerBeetle::ClientClosedError) { client.lookup_accounts([1]) }
  end

  def test_open_closes_after_block_raises
    client = nil
    assert_raises(RuntimeError) do
      TigerBeetle::Client.open(cluster_id: 0, replica_addresses: @tb_address) do |c|
        client = c
        raise "oops"
      end
    end

    assert_raises(TigerBeetle::ClientClosedError) { client.lookup_accounts([1]) }
  end

  def test_invalid_address_raises
    err = assert_raises(TigerBeetle::InitError) do
      TigerBeetle::Client.new(cluster_id: 0, replica_addresses: "not-an-address")
    end

    assert_equal("Init error: address_invalid", err.message)
  end

  def test_replica_addresses_must_be_a_string
    assert_raises(TypeError) do
      TigerBeetle::Client.new(cluster_id: 0, replica_addresses: [@tb_address])
    end
  end

  def test_multiple_clients_submit_from_multiple_threads
    clients = Array.new(2) do
      TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    end

    accounts = clients.map do
      TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1)
    end

    threads = clients.zip(accounts).map do |client, account|
      Thread.new { client.create_accounts([account]) }
    end

    results = threads.map(&:value)
    results.each do |result|
      assert_equal(1, result.length)
      assert_equal(TigerBeetle::CreateAccountStatus::CREATED, result[0].status)
    end

  ensure
    clients&.each(&:close)
  end

  def test_concurrent_close_create_and_lookup_does_not_crash_or_hang
    client_count = 5
    action_count = 50
    clients = Array.new(client_count) do
      TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    end
    account_ids = []

    threads = Array.new(action_count) do
      Thread.new do
        Thread.current.report_on_exception = false
        sleep(rand < 0.2 ? 0 : rand / 1000.0)
        client = clients.sample

        begin
          if rand < 0.1
            client.close
          elsif rand < 0.7
            id = TigerBeetle.id
            account_ids << id
            account = TigerBeetle::Account.new(id: id, ledger: 1, code: 1)
            client.create_accounts([account])
          else
            id = rand < 0.2 || account_ids.empty? ? TigerBeetle.id : account_ids.sample
            client.lookup_accounts([id])
          end
        rescue TigerBeetle::ClientClosedError
          nil
        end
      end
    end

    threads.each(&:value)

  ensure
    clients&.each(&:close)
  end
end
