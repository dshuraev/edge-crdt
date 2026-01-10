defmodule EdgeCrdtTest.Unit.DotTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Dot

  @replica_a "000000000000000a"
  @replica_b "000000000000000b"

  describe "new/2" do
    test "returns a dot when id is a binary and clock is a positive integer" do
      assert Dot.new(@replica_a, 1) == {@replica_a, 1}
      assert Dot.new(@replica_b, 42) == {@replica_b, 42}
    end

    test "raises when id is not a binary" do
      assert_raise FunctionClauseError, fn -> Dot.new(:replica, 1) end
    end

    test "raises when clock is not a positive integer" do
      assert_raise FunctionClauseError, fn -> Dot.new(@replica_a, 0) end
      assert_raise FunctionClauseError, fn -> Dot.new(@replica_a, -1) end
      assert_raise FunctionClauseError, fn -> Dot.new(@replica_a, 1.5) end
    end
  end
end
