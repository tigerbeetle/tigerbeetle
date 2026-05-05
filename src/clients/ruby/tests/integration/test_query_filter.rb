require "minitest/autorun"
require "tigerbeetle"

class TestQueryFilter < Minitest::Test
  BATCH_MAX = 8_189
  UINT64_MAX = (1 << 64) - 1

  def setup
    @tb_address = ENV.fetch("TB_ADDRESS", "3000")
    @client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
  end

  def teardown
    @client.close
  end

  def test_query_accounts_filters_and_paginates
    ledger = 1
    code = 999

    accounts = Array.new(10) do |i|
      TigerBeetle::Account.new(
        id: TigerBeetle.id,
        user_data_128: i.even? ? 1000 : 2000,
        user_data_64: i.even? ? 100 : 200,
        user_data_32: i.even? ? 10 : 20,
        ledger: ledger,
        code: code
      )
    end

    results = @client.create_accounts(accounts)
    assert_equal(10, results.length)
    assert_all(results) { it.status == TigerBeetle::CreateAccountStatus::CREATED }

    ascending = @client.query_accounts(
      TigerBeetle::QueryFilter.new(
        user_data_128: 1000,
        user_data_64: 100,
        user_data_32: 10,
        ledger: ledger,
        code: code,
        limit: BATCH_MAX
      )
    )
    assert_equal(5, ascending.length)
    assert_strictly_ascending(ascending.map(&:timestamp))
    assert_all(ascending) { it.user_data_128 == 1000 }
    assert_all(ascending) { it.user_data_64 == 100 }
    assert_all(ascending) { it.user_data_32 == 10 }
    assert_all(ascending) { it.ledger == ledger }
    assert_all(ascending) { it.code == code }

    reversed = @client.query_accounts(
      TigerBeetle::QueryFilter.new(
        user_data_128: 2000,
        user_data_64: 200,
        user_data_32: 20,
        ledger: ledger,
        code: code,
        limit: BATCH_MAX,
        flags: TigerBeetle::QueryFilterFlags::REVERSED
      )
    )
    assert_equal(5, reversed.length)
    assert_strictly_descending(reversed.map(&:timestamp))

    page_filter = TigerBeetle::QueryFilter.new(
      ledger: ledger,
      code: code,
      limit: 5,
      flags: TigerBeetle::QueryFilterFlags::REVERSED
    )
    first_page = @client.query_accounts(page_filter)
    assert_equal(5, first_page.length)
    assert_all(first_page) { it.ledger == ledger }
    assert_all(first_page) { it.code == code }

    page_filter.timestamp_max = first_page.last.timestamp - 1
    second_page = @client.query_accounts(page_filter)
    assert_equal(5, second_page.length)
    assert_all(second_page) { it.ledger == ledger }
    assert_all(second_page) { it.code == code }

    page_filter.timestamp_max = second_page.last.timestamp - 1
    assert_empty(@client.query_accounts(page_filter))
  end

  def test_query_transfers_filters_and_paginates
    ledger = 1
    code = 999
    debit_account_id = TigerBeetle.id
    credit_account_id = TigerBeetle.id

    @client.create_accounts(
      [
        TigerBeetle::Account.new(id: debit_account_id, ledger: ledger, code: 1),
        TigerBeetle::Account.new(id: credit_account_id, ledger: ledger, code: 1)
      ]
    )

    transfers = Array.new(10) do |i|
      TigerBeetle::Transfer.new(
        id: TigerBeetle.id,
        debit_account_id: debit_account_id,
        credit_account_id: credit_account_id,
        amount: 100,
        user_data_128: i.even? ? 1000 : 2000,
        user_data_64: i.even? ? 100 : 200,
        user_data_32: i.even? ? 10 : 20,
        ledger: ledger,
        code: code
      )
    end

    results = @client.create_transfers(transfers)
    assert_equal(10, results.length)
    assert_all(results) { it.status == TigerBeetle::CreateTransferStatus::CREATED }

    ascending = @client.query_transfers(
      TigerBeetle::QueryFilter.new(
        user_data_128: 1000,
        user_data_64: 100,
        user_data_32: 10,
        ledger: ledger,
        code: code,
        limit: BATCH_MAX
      )
    )
    assert_equal(5, ascending.length)
    assert_strictly_ascending(ascending.map(&:timestamp))
    assert_all(ascending) { it.user_data_128 == 1000 }
    assert_all(ascending) { it.user_data_64 == 100 }
    assert_all(ascending) { it.user_data_32 == 10 }
    assert_all(ascending) { it.ledger == ledger }
    assert_all(ascending) { it.code == code }

    reversed = @client.query_transfers(
      TigerBeetle::QueryFilter.new(
        user_data_128: 2000,
        user_data_64: 200,
        user_data_32: 20,
        ledger: ledger,
        code: code,
        limit: BATCH_MAX,
        flags: TigerBeetle::QueryFilterFlags::REVERSED
      )
    )
    assert_equal(5, reversed.length)
    assert_strictly_descending(reversed.map(&:timestamp))

    page_filter = TigerBeetle::QueryFilter.new(
      ledger: ledger,
      code: code,
      limit: 5,
      flags: TigerBeetle::QueryFilterFlags::REVERSED
    )
    first_page = @client.query_transfers(page_filter)
    assert_equal(5, first_page.length)
    assert_all(first_page) { it.ledger == ledger }
    assert_all(first_page) { it.code == code }

    page_filter.timestamp_max = first_page.last.timestamp - 1
    second_page = @client.query_transfers(page_filter)
    assert_equal(5, second_page.length)
    assert_all(second_page) { it.ledger == ledger }
    assert_all(second_page) { it.code == code }

    page_filter.timestamp_max = second_page.last.timestamp - 1
    assert_empty(@client.query_transfers(page_filter))
  end

  def test_query_operations_return_empty_results
    filter = TigerBeetle::QueryFilter.new(
      user_data_128: TigerBeetle.id,
      ledger: 1,
      code: 999,
      limit: BATCH_MAX
    )

    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))
  end

  def test_invalid_query_filters
    filter = TigerBeetle::QueryFilter.new(timestamp_min: UINT64_MAX, limit: BATCH_MAX)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    filter = TigerBeetle::QueryFilter.new(timestamp_max: UINT64_MAX, limit: BATCH_MAX)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    filter = TigerBeetle::QueryFilter.new(
      timestamp_min: UINT64_MAX - 1,
      timestamp_max: 1,
      limit: BATCH_MAX
    )
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    filter = TigerBeetle::QueryFilter.new(limit: 0)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    filter = TigerBeetle::QueryFilter.new(limit: BATCH_MAX, flags: 0xFFFF)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    too_much_data = TigerBeetle::QueryFilter.new(limit: 10_000)
    assert_raises(TigerBeetle::PacketError) { @client.query_accounts(too_much_data) }
    assert_raises(TigerBeetle::PacketError) { @client.query_transfers(too_much_data) }
  end

  def test_query_operations_raise_after_close
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    client.close
    filter = TigerBeetle::QueryFilter.new(limit: 1)

    assert_raises(TigerBeetle::ClientClosedError) { client.query_accounts(filter) }
    assert_raises(TigerBeetle::ClientClosedError) { client.query_transfers(filter) }
  end

  private

  def assert_all(collection)
    collection.each_with_index do |item, index|
      assert(yield(item), "Expected item #{index} to match")
    end
  end

  def assert_strictly_ascending(values)
    values.each_cons(2) { |a, b| assert_operator(a, :<, b) }
  end

  def assert_strictly_descending(values)
    values.each_cons(2) { |a, b| assert_operator(a, :>, b) }
  end
end
