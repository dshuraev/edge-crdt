defmodule EdgeCrdtTest.Unit.ReplicaComponentsTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.Components

  describe "append/5" do
    test "stores deltas keyed by counter per origin" do
      cs0 = Components.new()

      cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :d1)
      assert Components.origins(cs1, "crdt-1") == ["replica-a"]

      cs2 = Components.append(cs1, "crdt-1", "replica-a", 3, :d3)
      assert Map.new(Map.fetch!(Components.since(cs2, "crdt-1", "replica-a", 0), "crdt-1")) ==
               %{{"replica-a", 1} => :d1, {"replica-a", 3} => :d3}

      assert Map.new(Map.fetch!(Components.since(cs2, "crdt-1", "replica-a", 1), "crdt-1")) ==
               %{{"replica-a", 3} => :d3}
    end

    test "returns {:error, :duplicate} when counter already exists" do
      cs0 = Components.new()
      cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :d1)

      assert {:error, :duplicate} = Components.append(cs1, "crdt-1", "replica-a", 1, :d1_again)
    end

    test "accepts non-monotonic counters" do
      cs0 = Components.new()
      cs1 = Components.append(cs0, "crdt-1", "replica-a", 3, :d3)
      cs2 = Components.append(cs1, "crdt-1", "replica-a", 2, :d2)

      assert Map.new(Map.fetch!(Components.since(cs2, "crdt-1", "replica-a", 0), "crdt-1")) ==
               %{{"replica-a", 2} => :d2, {"replica-a", 3} => :d3}
    end
  end

  test "supports multiple origins per CRDT" do
    cs0 = Components.new()

    cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :a1)
    cs2 = Components.append(cs1, "crdt-1", "replica-b", 2, :b2)

    assert Enum.sort(Components.origins(cs2, "crdt-1")) == ["replica-a", "replica-b"]
  end
end
