defmodule EdgeCrdtTest.Unit.DeltaBundleTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.DeltaBundle

  @replica_a "000000000000000a"
  @replica_b "000000000000000b"
  @crdt_1 "1111111111111111"
  @crdt_2 "2222222222222222"

  test "combine/2 merges bundles" do
    a = %{@crdt_1 => [{{@replica_a, 1}, :d1}]}
    b = %{@crdt_2 => [{{@replica_b, 1}, :x1}]}

    assert DeltaBundle.combine(a, b) == %{
             @crdt_1 => [{{@replica_a, 1}, :d1}],
             @crdt_2 => [{{@replica_b, 1}, :x1}]
           }
  end

  test "combine/2 concatenates lists for the same CRDT" do
    a = %{@crdt_1 => [{{@replica_a, 1}, :d1}]}
    b = %{@crdt_1 => [{{@replica_a, 2}, :d2}]}

    assert DeltaBundle.combine(a, b) == %{
             @crdt_1 => [{{@replica_a, 1}, :d1}, {{@replica_a, 2}, :d2}]
           }
  end
end
