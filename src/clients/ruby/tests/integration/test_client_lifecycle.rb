require "minitest/autorun"
require "tigerbeetle"

class TestClientLifecycle < Minitest::Test
  def setup
    @tb_address = ENV.fetch("TB_ADDRESS", "3000")
  end

  def test_connect_and_close
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    refute_predicate(client, :closed?)

    client.close
    assert_predicate(client, :closed?)
  end

  def test_double_close_raises
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    client.close
    assert_raises(TigerBeetle::ClientClosedError) { client.close }
  end

  def test_native_submit_after_close_raises
    read_io, write_io = IO.pipe
    # NativeClient is hidden from users, but we still want some tests around it.
    native_client = TigerBeetle.const_get(:NativeClient)
    native = native_client.new(0, @tb_address, write_io.fileno)
    native.close

    assert_raises(TigerBeetle::ClientClosedError) do
      native.submit(TigerBeetle::Operation::LOOKUP_ACCOUNTS, [1])
    end

  ensure
    read_io&.close
    write_io&.close
  end

  def test_open_closes_after_block
    client = nil
    TigerBeetle::Client.open(cluster_id: 0, replica_addresses: @tb_address) do |c|
      client = c
      refute_predicate(client, :closed?)
    end

    assert_predicate(client, :closed?)
  end

  def test_open_closes_after_block_raises
    client = nil
    assert_raises(RuntimeError) do
      TigerBeetle::Client.open(cluster_id: 0, replica_addresses: @tb_address) do |c|
        client = c
        raise "oops"
      end
    end

    assert_predicate(client, :closed?)
  end

  def test_invalid_address_raises
    err = assert_raises(TigerBeetle::InitError) do
      TigerBeetle::Client.new(cluster_id: 0, replica_addresses: "not-an-address")
    end

    assert_equal("Init error: address_invalid", err.message)
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
end
