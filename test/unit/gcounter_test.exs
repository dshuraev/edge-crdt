defmodule EdgeCrdtTest.Unit.GCounterTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Crdt.GCounter
  alias EdgeCrdt.Replica.Context

  @replica_a "000000000000000a"
  @replica_b "000000000000000b"

  test "zero/0 returns a fresh counter with value 0" do
    assert 0 == Crdt.value(GCounter, Crdt.zero(GCounter))
  end

  test "mutate/3 increments the local replica component and returns a minimal delta" do
    state = Crdt.zero(GCounter)

    assert {:ok, {state1, delta}} = Crdt.mutate(GCounter, state, :inc, {@replica_a, 1})
    assert 1 == Crdt.value(GCounter, state1)

    assert {:ok, {state2, delta2}} =
             Crdt.mutate(GCounter, state1, {:inc, 2}, {@replica_a, 2})
    assert 3 == Crdt.value(GCounter, state2)

    assert 3 == Crdt.value(GCounter, delta2)
    assert {:ok, ^state1} = Crdt.apply_delta(GCounter, Crdt.zero(GCounter), delta, Context.new())
  end

  test "apply_delta/3 is monotonic per replica (max) and updates the total" do
    state = Crdt.zero(GCounter)
    ctx = Context.new()

    assert {:ok, {state, delta}} = Crdt.mutate(GCounter, state, {:inc, 5}, {@replica_a, 1})
    assert {:ok, state} = Crdt.apply_delta(GCounter, state, delta, ctx)
    assert 5 == Crdt.value(GCounter, state)

    assert {:ok, state} = Crdt.apply_delta(GCounter, state, Crdt.zero(GCounter), ctx)
    assert 5 == Crdt.value(GCounter, state)
  end

  test "join/2 takes per-replica max and is commutative" do
    ctx = Context.new()

    left = Crdt.zero(GCounter)
    right = Crdt.zero(GCounter)

    assert {:ok, {left, _}} = Crdt.mutate(GCounter, left, {:inc, 2}, {@replica_a, 1})
    assert {:ok, {right, _}} = Crdt.mutate(GCounter, right, {:inc, 5}, {@replica_a, 1})
    assert {:ok, {right, _}} = Crdt.mutate(GCounter, right, {:inc, 1}, {@replica_b, 1})

    assert {:ok, joined1} = Crdt.join(GCounter, left, right)
    assert {:ok, joined2} = Crdt.join(GCounter, right, left)
    assert 6 == Crdt.value(GCounter, joined1)
    assert 6 == Crdt.value(GCounter, joined2)

    assert {:ok, ^joined1} = Crdt.apply_delta(GCounter, left, right, ctx)
  end
end
