require "minitest/autorun"
require "tigerbeetle"

class TestTransfers < Minitest::Test
  def setup
    tb_address = ENV.fetch("TB_ADDRESS", "3000")
    @client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: tb_address)

    @a1_id = TigerBeetle.generate_id
    @a2_id = TigerBeetle.generate_id
    @client.create_accounts(
      [
        TigerBeetle::Account.new do |a|
          a.id = @a1_id
          a.ledger = 1
          a.code = 1
        end,
        TigerBeetle::Account.new do |a|
          a.id = @a2_id
          a.ledger = 1
          a.code = 1
        end
      ]
    )
  end

  def teardown
    @client.close
  end

  def test_create_transfer
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 100
          t.ledger = 1
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_operator(results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, results[0].status)
  end

  def test_create_transfer_duplicate
    transfer = TigerBeetle::Transfer.new do |t|
      t.id = TigerBeetle.generate_id
      t.debit_account_id = @a1_id
      t.credit_account_id = @a2_id
      t.amount = 10
      t.ledger = 1
      t.code = 1
    end

    @client.create_transfers([transfer])

    results = @client.create_transfers([transfer])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::EXISTS, results[0].status)
  end

  def test_create_transfer_id_zero
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = 0
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 10
          t.ledger = 1
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::ID_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_transfer_debit_account_id_zero
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = 0
          t.credit_account_id = @a2_id
          t.amount = 10
          t.ledger = 1
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_transfer_credit_account_id_zero
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = @a1_id
          t.credit_account_id = 0
          t.amount = 10
          t.ledger = 1
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_transfer_accounts_must_be_different
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a1_id
          t.amount = 10
          t.ledger = 1
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::ACCOUNTS_MUST_BE_DIFFERENT, results[0].status)
  end

  def test_create_transfer_ledger_zero
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 10
          t.ledger = 0
          t.code = 1
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::LEDGER_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_transfer_code_zero
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new do |t|
          t.id = TigerBeetle.generate_id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 10
          t.ledger = 1
          t.code = 0
        end
      ]
    )
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateTransferStatus::CODE_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_lookup_transfer
    id = TigerBeetle.generate_id
    @client.create_transfers(
      [
        TigerBeetle::Transfer.new { |t|
          t.id = id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 42
          t.ledger = 1
          t.code = 1
        }
      ]
    )

    results = @client.lookup_transfers([id])
    assert_equal(1, results.length)
    assert_equal(id, results[0].id)
    assert_equal(42, results[0].amount)
  end

  def test_lookup_transfer_not_found
    id = TigerBeetle.generate_id

    results = @client.lookup_transfers([id])
    assert_equal(0, results.length)
  end

  def test_lookup_transfers_multiple
    id1 = TigerBeetle.generate_id
    id2 = TigerBeetle.generate_id
    @client.create_transfers(
      [
        TigerBeetle::Transfer.new { |t|
          t.id = id1
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 10
          t.ledger = 1
          t.code = 1
        },
        TigerBeetle::Transfer.new { |t|
          t.id = id2
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 20
          t.ledger = 1
          t.code = 1
        }
      ]
    )

    results = @client.lookup_transfers([id1, id2])
    assert_equal(2, results.length)
    r1 = results.find { it.id == id1 }
    r2 = results.find { it.id == id2 }
    refute_nil(r1)
    refute_nil(r2)
    assert_equal(10, r1.amount)
    assert_equal(20, r2.amount)
  end

  def test_lookup_transfers_partial_match
    existing_id = TigerBeetle.generate_id
    missing_id = TigerBeetle.generate_id
    @client.create_transfers(
      [
        TigerBeetle::Transfer.new { |t|
          t.id = existing_id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 5
          t.ledger = 1
          t.code = 1
        }
      ]
    )

    results = @client.lookup_transfers([existing_id, missing_id])
    assert_equal(1, results.length)
    assert_equal(existing_id, results[0].id)
  end

  def test_lookup_transfers_empty_batch
    results = @client.lookup_transfers([])
    assert_equal(0, results.length)
  end

  def test_lookup_transfer_field_roundtrip
    id = TigerBeetle.generate_id
    user_data_128 = TigerBeetle.generate_id
    user_data_64 = 8_888_888_888
    user_data_32 = 54321
    @client.create_transfers(
      [
        TigerBeetle::Transfer.new { |t|
          t.id = id
          t.debit_account_id = @a1_id
          t.credit_account_id = @a2_id
          t.amount = 99
          t.ledger = 1
          t.code = 7
          t.user_data_128 = user_data_128
          t.user_data_64 = user_data_64
          t.user_data_32 = user_data_32
        }
      ]
    )

    results = @client.lookup_transfers([id])
    assert_equal(1, results.length)
    t = results[0]
    assert_equal(id, t.id)
    assert_equal(@a1_id, t.debit_account_id)
    assert_equal(@a2_id, t.credit_account_id)
    assert_equal(99, t.amount)
    assert_equal(1, t.ledger)
    assert_equal(7, t.code)
    assert_equal(user_data_128, t.user_data_128)
    assert_equal(user_data_64, t.user_data_64)
    assert_equal(user_data_32, t.user_data_32)
    assert_operator(t.timestamp, :>, 0)
  end
end
