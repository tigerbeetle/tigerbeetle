require "ffi"

require_relative "bindings"
require_relative "request"

module TigerBeetle
  class Client
    def initialize(addresses:, cluster_id:, client_id: 1)
      @addresses = addresses.to_s
      @cluster_id = cluster_id
      @client_id = client_id
      @inflight_requests = {}
      @on_completion = Proc.new { |*args| callback(*args) }

      at_exit do
        if @client
          Bindings.tb_client_register_log_callback(nil, false)
          Bindings.tb_client_deinit(@client)
          @client = nil
          @inflight_requests = {}
        end
      end
    end

    def connect
      raise "Already connected" if @client

      @client = Bindings::Client.new

      Bindings.tb_client_init(
        @client,
        @cluster_id,
        @addresses,
        @addresses.length,
        @client_id,
        @on_completion
      )
    end

    def register_logger(logger)
      raise "Logger already registered" if @log_handler && !logger.nil?

      if logger.nil?
        Bindings.tb_client_register_log_callback(nil, false)
      else
        @log_handler = Proc.new do |level, ptr, length|
          logger.log(level, ptr.read_bytes(length))
        end
        Bindings.tb_client_register_log_callback(@log_handler, true)
      end
    end

    def submit(packet, result_type, &result_block)
      request_id = packet[:user_data].read_uint64
      queue = SizedQueue.new(1)

      @inflight_requests[request_id] = Request.new(packet, result_type) do |result|
        if result_block
          result_block.call(result)
        else
          queue << result
        end
      end

      status = Bindings.tb_client_submit(@client, packet)

      raise "Failed to submit packet: #{status}" unless status == Bindings::ClientStatus[:OK]

      queue.pop unless result_block
    end

    private

    def callback(client_id, packet, timestamp, result_ptr, result_len)
      request_id = packet[:user_data].read_uint64
      request = @inflight_requests.delete(request_id)
      raise "Request not found for ID: #{request_id}" unless request.is_a?(Request)

      result_type = request.result_type
      results = Array.new(result_len / result_type.size) do |i|
        ptr = FFI::MemoryPointer.new(result_type, 1)
        ptr.put_bytes(0, result_ptr.get_bytes(i * result_type.size, result_type.size))
        result_type.new(ptr)
      end

      request.block.call(results)
    end
  end
end
