require "minitest/autorun"
require "tigerbeetle"

class TestID < Minitest::Test
  def test_unique
    ids = Array.new(1000) { TigerBeetle.id }
    assert_equal(ids.length, ids.uniq.length)
  end

  def test_monotonic
    ids = Array.new(100) { TigerBeetle.id }
    assert_equal(ids, ids.sort)
  end

  def test_fits_in_128_bits
    1000.times do
      id = TigerBeetle.id
      assert(id >= 0)
      assert(id < 2 ** 128)
    end
  end

  def test_random_overflow_advances_timestamp
    gen = generator_in_future_with_random(TigerBeetle::ID::RANDOM_MAX - 1)
    last_ms = gen.instance_variable_get(:@last_ms)

    id = gen.generate

    new_ms = gen.instance_variable_get(:@last_ms)
    assert_equal(last_ms + 1, new_ms, "timestamp must advance on random overflow")
    assert_equal(0, gen.instance_variable_get(:@random))
    assert_equal(new_ms << 80, id)
  end

  def test_random_near_max_does_not_raise
    gen = generator_in_future_with_random(TigerBeetle::ID::RANDOM_MAX - 2)

    assert_equal(TigerBeetle::ID::RANDOM_MAX - 1, gen.generate & (TigerBeetle::ID::RANDOM_MAX - 1))
  end

  def test_ids_monotonic_across_random_overflow
    gen = generator_in_future_with_random(TigerBeetle::ID::RANDOM_MAX - 2)

    ids = Array.new(5) { gen.generate }
    assert_equal(ids, ids.sort)
    assert_equal(ids.length, ids.uniq.length)
  end

  private

  # Creates an ID generator in the future so the ms don't increment before the random overflows.
  def generator_in_future_with_random(random)
    future_ms = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond) + 1_000_000
    TigerBeetle::ID.new.tap do |gen|
      gen.instance_variable_set(:@last_ms, future_ms)
      gen.instance_variable_set(:@random, random)
    end
  end
end
