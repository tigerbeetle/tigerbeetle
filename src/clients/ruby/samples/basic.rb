#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "tigerbeetle"

addresses = ENV.fetch("TB_ADDRESS", "3000")
cluster_id = ENV.fetch("TB_CLUSTER_ID", "0").to_i

client = TigerBeetle.connect(addresses:, cluster_id:)

LEDGER = 1
CODE = 1

accounts = [
  TigerBeetle::Bindings::Account.new.tap do |account|
    account[:id] = TigerBeetle::Types::UINT128.to_native(100, nil)
    account[:ledger] = LEDGER
    account[:code] = CODE
  end,
  TigerBeetle::Bindings::Account.new.tap do |account|
    account[:id] = TigerBeetle::Types::UINT128.to_native(100, nil)
    account[:ledger] = LEDGER
    account[:code] = CODE
  end
]

account_data = FFI::MemoryPointer.new(TigerBeetle::Bindings::Account, accounts.size)
accounts.each_with_index do |account, index|
  account_data.put_bytes(index * TigerBeetle::Bindings::Account.size, account.read_bytes(TigerBeetle::Bindings::Account.size))
end

user_data = FFI::MemoryPointer.new(FFI::Type::UINT64, 1)
user_data.write_uint64(2)

packet = TigerBeetle::Bindings::Packet.new.tap do |p|
  p[:user_data] = user_data
  p[:data] = account_data
  p[:data_size] = account_data.size
  p[:status] = TigerBeetle::Bindings::PacketStatus[:OK]
  p[:operation] = TigerBeetle::Bindings::Operation[:CREATE_ACCOUNTS]
end

account_errors = client.submit(packet, TigerBeetle::Bindings::CreateAccountsResult)
raise "Error: #{account_errors}" unless account_errors.empty?
