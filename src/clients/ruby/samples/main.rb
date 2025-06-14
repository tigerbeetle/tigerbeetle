#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "tigerbeetle"

PORT = ENV.fetch("TB_PORT", "3000").to_s
CLUSTER_ID = ENV.fetch("TB_CLUSTER_ID", "0").to_i
ledger = ENV.fetch("TB_LEDGER", "1").to_i
code = ENV.fetch("TB_CODE", "1").to_i

client = TigerBeetle.connect(addresses: PORT, cluster_id: CLUSTER_ID)

account1 = TigerBeetle::Bindings::Account.new(
  id: "1", ledger:, code:
)
# account1 = TigerBeetle::Bindings::Account.new(
#   id: "1", ledger:, code:, flags: TigerBeetle::Bindings::AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS
# )
account2 = TigerBeetle::Bindings::Account.new(
  id: "1", ledger:, code:, flags: TigerBeetle::Bindings::AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS
)

results = client.lookup_accounts([1, 2]) do |res|
  puts res
end
puts results_queue.pop

client.create_accounts(account1, account2) do |results|
  puts "here"
  results_queue << results
end

results = results_queue.pop

puts "Created accounts: #{results}"
raise "missing results" if results.size != 2

results.each do |result|
  raise "result failed" if !results.success?
end

puts 'created accounts'


