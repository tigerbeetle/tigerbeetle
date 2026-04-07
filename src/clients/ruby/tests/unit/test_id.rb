require "minitest/autorun"
require "tigerbeetle"

class TestID < Minitest::Test
  def test_unique
    ids = Array.new(1000) { TigerBeetle.generate_id }
    assert_equal(ids.length, ids.uniq.length)
  end

  def test_monotonic
    ids = Array.new(100) { TigerBeetle.generate_id }
    assert_equal(ids, ids.sort)
  end

  def test_fits_in_128_bits
    1000.times do
      id = TigerBeetle.generate_id
      assert(id >= 0)
      assert(id < 2 ** 128)
    end
  end
end
