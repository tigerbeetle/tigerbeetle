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

    assert_match(/invalid replica address/, err.message)
  end
end
