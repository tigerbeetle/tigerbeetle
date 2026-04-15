require "securerandom"

module TigerBeetle
  class IdGenerator
    RANDOM_MAX = 2 ** 80
    TIMESTAMP_MAX = 2 ** 48

    def initialize
      @last_ms = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
      @random = next_random
    end

    def generate
      ms = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
      if ms <= @last_ms
        ms = @last_ms
      else
        @last_ms = ms
        @random = next_random
      end

      @random += 1
      if @random >= RANDOM_MAX
        @last_ms += 1
        raise "Timestamp bits overflow on monotonic increment" if @last_ms >= TIMESTAMP_MAX
        @random = 0
      end

      (@last_ms << 80) | @random
    end

    private

    def next_random
      # See: https://docs.ruby-lang.org/en/4.0/language/packed_data_rdoc.html#label-For+Integers
      lo, hi = SecureRandom.bytes(10).unpack("Q<S<")
      lo | (hi << 64)
    end
  end
end
