defmodule EdgeCrdtTest.Unit.ReplicaStateTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.State

  defmodule TestCrdt do
    def zero(), do: :zero
  end

  defmodule NoZeroCrdt do
  end

  describe "ensure_crdt/4" do
    test "returns {:ok, state} when CRDT already exists" do
      state0 = State.new("replica-a")

      assert {:ok, state1} = State.ensure_crdt(state0, "crdt-1", TestCrdt)
      assert {:ok, state2} = State.ensure_crdt(state1, "crdt-1", TestCrdt)
      assert state1 == state2
    end

    test "returns {:error, reason} when CRDT cannot be created" do
      state = State.new("replica-a")

      assert {:error, {:implementation_missing, NoZeroCrdt, [{:zero, 0}]}} =
               State.ensure_crdt(state, "crdt-1", NoZeroCrdt)
    end
  end
end
