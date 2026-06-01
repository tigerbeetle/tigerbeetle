module TigerBeetle
  class CompletionDispatcher
    COMPLETION_ID_BYTES = 8

    class Completion
      def initialize
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @done = false
      end

      def done?
        @done
      end

      def wait
        @mutex.synchronize { @condition.wait(@mutex) until done? }
      end

      def complete
        @mutex.synchronize do
          @done = true
          @condition.broadcast
        end
      end
    end

    def initialize
      @read_io, @write_io = IO.pipe
      @pending = {}
      @completed_ids = {}
      @cancelled_ids = {}
      @mutex = Mutex.new
      @thread = Thread.new { dispatch_completions }
    end

    def write_fileno
      @write_io.fileno
    end

    def completion_for(request_id)
      completion = Completion.new
      completion.complete if register(request_id, completion)
      completion
    end

    def submit_and_wait_for(native, operation, payload)
      request = native.submit(operation, payload)
      completion = completion_for(request.id)
      completion.wait
      request
    ensure
      unregister(request.id, completion) if request
    end

    def unregister(request_id, completion)
      @mutex.synchronize do
        @pending.delete(request_id)
        @completed_ids.delete(request_id)
        @cancelled_ids[request_id] = true unless completion&.done?
      end
    end

    private

    def register(request_id, completion)
      @mutex.synchronize do
        if @completed_ids.delete(request_id)
          true
        else
          @pending[request_id] = completion
          false
        end
      end
    end

    def dispatch_completions
      loop do
        request_id = read_completion_id
        completion = @mutex.synchronize do
          completion = @pending[request_id]
          if completion
            completion
          elsif @cancelled_ids.delete(request_id)
            nil
          else
            @completed_ids[request_id] = true
            nil
          end
        end

        completion&.complete
      end

    rescue IOError, ClientClosedError
      nil
    end

    def read_completion_id
      bytes = @read_io.read(COMPLETION_ID_BYTES)
      raise ClientClosedError unless bytes

      bytes.unpack1("Q")
    end
  end

  private_constant :CompletionDispatcher
end
