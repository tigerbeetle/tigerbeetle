# frozen_string_literal: true

require "timeout"
require_relative "bindings"

module TigerBeetle
  class Client
    def initialize(addresses: "3000", cluster_id: 0)
      @addresses = addresses.to_s
      @cluster_id = cluster_id.to_i
      @client = Bindings::Client.new
    end

    def connect
      @client.init(@addresses, @cluster_id)
    end

    # CreateAccounts(accounts []types.Account) ([]types.AccountEventResult, error)
    def create_accounts(*accounts)
      accounts = array_wrap(accounts)

      client.submit(Bindings::Operation::CREATE_ACCOUNTS, accounts)
    end

    # CreateTransfers(transfers []types.Transfer) ([]types.TransferEventResult, error)
    def create_transfers(transfers)
    end

    # LookupAccounts(accountIDs []types.Uint128) ([]types.Account, error)
    # @param account_ids [Array, Integer] The account IDs to look up.
    def lookup_accounts(account_ids)
      account_ids = array_wrap(account_ids)

      client.submit(Bindings::Operation::LOOKUP_ACCOUNTS, account_ids)
    end

    # LookupTransfers(transferIDs []types.Uint128) ([]types.Transfer, error)
    def lookup_transfers(transfer_ids)
    end

    # GetAccountTransfers(filter types.AccountFilter) ([]types.Transfer, error)
    def get_account_transfers(filter)
    end

    # GetAccountBalances(filter types.AccountFilter) ([]types.AccountBalance, error)
    def get_account_balances(filter)
    end

    # QueryAccounts(filter types.QueryFilter) ([]types.Account, error)
    def query_accounts(filter)
    end

    # QueryTransfers(filter types.QueryFilter) ([]types.Transfer, error)
    def query_transfers(filter)
    end

    private

    attr_reader :client

    def array_wrap(args)
      if args.nil?
        []
      elsif args.respond_to?(:to_ary)
        args
      else
        [args]
      end
    end
  end
end
