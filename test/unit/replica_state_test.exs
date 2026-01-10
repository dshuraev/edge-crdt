defmodule EdgeCrdtTest.Unit.ReplicaStateTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Crdt.GCounter
  alias EdgeCrdt.Replica.State

  @replica_id "000000000000000a"
  @crdt_1 "1111111111111111"
  @crdt_2 "2222222222222222"

  defmodule TestCrdt do
    def zero, do: :zero
    def version, do: 0
    def encode(_state), do: {:ok, <<0::16, 0::32>>}
    def decode(_encoded), do: {:ok, :zero}
    def value(_state), do: :zero
    def mutate(state, _op, _dot), do: {:ok, {state, :delta}}
    def join(left, _right), do: {:ok, left}
    def context(_state), do: EdgeCrdt.Replica.Context.new()
    def apply_delta(state, _delta, _ctx), do: {:ok, state}
  end

  defmodule NoZeroCrdt do
  end

  describe "ensure_crdt/4" do
    test "returns {:ok, state} when CRDT already exists" do
      state0 = State.new(@replica_id)

      assert {:ok, state1} = State.ensure_crdt(state0, @crdt_1, TestCrdt)
      assert {:ok, state2} = State.ensure_crdt(state1, @crdt_1, TestCrdt)
      assert state1 == state2
    end

    test "returns {:error, reason} when CRDT cannot be created" do
      state = State.new(@replica_id)

      assert {:error, {:implementation_missing, NoZeroCrdt, missing}} =
               State.ensure_crdt(state, @crdt_1, NoZeroCrdt)

      assert {:zero, 0} in missing
    end
  end

  describe "digest/1" do
    test "returns max local counter per CRDT" do
      Code.ensure_loaded!(GCounter)
      state0 = State.new(@replica_id)

      assert {:ok, state1} = State.add_crdt(state0, @crdt_1, GCounter)
      assert {:ok, state2} = State.add_crdt(state1, @crdt_2, GCounter)

      assert %{
               @crdt_1 => {@replica_id, 0},
               @crdt_2 => {@replica_id, 0}
             } = State.digest(state2)

      assert {:ok, state3} = State.apply_op(state2, @crdt_1, :inc)
      assert {:ok, state4} = State.apply_op(state3, @crdt_2, :inc)
      assert {:ok, state5} = State.apply_op(state4, @crdt_1, :inc)

      assert %{
               @crdt_1 => {@replica_id, 3},
               @crdt_2 => {@replica_id, 3}
             } = State.digest(state5)
    end
  end

  describe "delta/2" do
    test "returns local deltas since digest counter per CRDT" do
      Code.ensure_loaded!(GCounter)
      state0 = State.new(@replica_id)

      assert {:ok, state1} = State.add_crdt(state0, @crdt_1, GCounter)
      assert {:ok, state2} = State.add_crdt(state1, @crdt_2, GCounter)

      assert {:ok, state3} = State.apply_op(state2, @crdt_1, :inc)
      assert {:ok, state4} = State.apply_op(state3, @crdt_2, :inc)
      assert {:ok, state5} = State.apply_op(state4, @crdt_1, :inc)

      all =
        State.delta(state5, %{
          @crdt_1 => {@replica_id, 0},
          @crdt_2 => {@replica_id, 0}
        })

      assert Map.new(Map.fetch!(all, @crdt_1)) ==
               %{
                 {@replica_id, 1} => %{@replica_id => 1},
                 {@replica_id, 3} => %{@replica_id => 2}
               }

      assert Map.new(Map.fetch!(all, @crdt_2)) ==
               %{
                 {@replica_id, 2} => %{@replica_id => 1}
               }

      since_2 =
        State.delta(state5, %{
          @crdt_1 => {@replica_id, 2},
          @crdt_2 => {@replica_id, 2}
        })

      assert Map.keys(since_2) == [@crdt_1]

      assert Map.new(Map.fetch!(since_2, @crdt_1)) ==
               %{
                 {@replica_id, 3} => %{@replica_id => 2}
               }
    end

    test "defaults to counter 0 for unknown CRDTs in digest" do
      Code.ensure_loaded!(GCounter)
      state0 = State.new(@replica_id)

      assert {:ok, state1} = State.add_crdt(state0, @crdt_1, GCounter)
      assert {:ok, state2} = State.apply_op(state1, @crdt_1, :inc)

      digest_before = State.digest(state2)

      assert {:ok, state3} = State.add_crdt(state2, @crdt_2, GCounter)
      assert {:ok, state4} = State.apply_op(state3, @crdt_2, :inc)

      bundle = State.delta(state4, Map.delete(digest_before, @crdt_2))

      assert Map.has_key?(bundle, @crdt_2)
      assert Map.new(Map.fetch!(bundle, @crdt_2)) ==
               %{{@replica_id, 2} => %{@replica_id => 1}}
    end
  end
end
