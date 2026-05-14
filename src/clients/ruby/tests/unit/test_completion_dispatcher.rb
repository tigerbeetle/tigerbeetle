require "minitest/autorun"
require "timeout"
require "tigerbeetle"

class TestCompletionDispatcher < Minitest::Test
  TestRequest = Struct.new(:id, keyword_init: true)

  # Dummy native client that just returns the unmodified request
  Native = Struct.new(:request, keyword_init: true) do
    def submit(_operation, _payload)
      request
    end
  end

  def setup
    # The dispatcher is hidden from users of the client but we want to run some tests against it so
    # we fetch the private constant with const_get.
    @dispatcher = TigerBeetle.const_get(:CompletionDispatcher).new
  end

  def teardown
    @dispatcher.instance_variable_get(:@write_io).close
    @dispatcher.instance_variable_get(:@read_io).close
    @dispatcher.instance_variable_get(:@thread).join(1)
  end

  def test_submit_and_wait_unregisters_request_when_waiting_thread_is_interrupted
    request = TestRequest.new(id: 1)
    thread = Thread.new do
      Thread.current.report_on_exception = false
      @dispatcher.submit_and_wait_for(
        Native.new(request: request),
        TigerBeetle::Operation::LOOKUP_ACCOUNTS,
        [1]
      )
    end

    wait_until { pending_ids.include?(request.id) }
    thread.raise("interrupted while waiting")
    wait_for_interrupted_thread(thread)

    refute_includes(pending_ids, request.id)
    assert_includes(cancelled_ids, request.id)

    write_completion_id(request.id)
    wait_until { !cancelled_ids.include?(request.id) }

    refute_includes(cancelled_ids, request.id)
    refute_includes(completed_ids, request.id)
    refute_includes(pending_ids, request.id)
  end

  def test_submit_and_wait_cleans_bookkeeping_after_normal_completion
    request = TestRequest.new(id: 1)
    thread = Thread.new do
      @dispatcher.submit_and_wait_for(
        Native.new(request: request),
        TigerBeetle::Operation::LOOKUP_ACCOUNTS,
        [1]
      )
    end

    wait_until { pending_ids.include?(request.id) }
    write_completion_id(request.id)

    assert_same(request, thread.value)
    refute_includes(pending_ids, request.id)
    refute_includes(cancelled_ids, request.id)
    refute_includes(completed_ids, request.id)
  end

  private

  def write_completion_id(request_id)
    write_io = @dispatcher.instance_variable_get(:@write_io)
    write_io.write([request_id].pack("Q"))
  end

  def wait_for_interrupted_thread(thread)
    thread.value
  rescue RuntimeError
    nil
  end

  def pending_ids
    @dispatcher.instance_variable_get(:@pending).keys
  end

  def completed_ids
    @dispatcher.instance_variable_get(:@completed_ids).keys
  end

  def cancelled_ids
    @dispatcher.instance_variable_get(:@cancelled_ids).keys
  end

  def wait_until
    Timeout.timeout(1) do
      loop do
        return if yield

        Thread.pass
      end
    end
  end
end
