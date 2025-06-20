require "bundler/setup"
require "tb_client"

# TODO
# 1. create accounts
# 2. ensure accounts created
# 3. create transfers
# 4. ensure no transfer errors
# 5. lookup accounts
# 6. confirm account debits and credits

ADDRESS = ENV.fetch("TB_ADDRESS", "3000")
CLUSTER_ID = ENV.fetch("TB_CLUSTER_ID", 0).to_i

include TBClient

cluster_id_ptr = UINT128.new
cluster_id_ptr[:lo] = CLUSTER_ID % 2**64
cluster_id_ptr[:hi] = CLUSTER_ID >> 64

client = Client.new

def completion(client_id, packet, timestamp, result_ptr, result_len)
  # request_id = packet[:user_data].read_uint64
  # request = inflight_requests[request_id]
  # result = deserialize(result_ptr, request.converter, result_len)
  # request.block.call(result)
  # inflight_requests.delete(request_id)
end

callback = Proc.new { |*args| completion(*args) }

init_status = tb_client_init(client, cluster_id_ptr, ADDRESS, ADDRESS.length, 0, &callback)

if InitStatus.find(init_status) == :SUCCESS
  puts "Conected"
else
  puts "Could not connect, status: #{InitStatus.find(init_status)}"
  exit 1
end
