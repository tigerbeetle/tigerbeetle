require "minitest/autorun"

class TestTBAlias < Minitest::Test
  def test_tb_alias_needs_to_be_explicitly_required
    require "tigerbeetle"

    assert_equal(Object.const_defined?(:TB), false)

    require "tigerbeetle/tb"

    assert_equal(Object.const_defined?(:TB), true)
  end
end
