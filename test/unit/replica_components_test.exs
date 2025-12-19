defmodule EdgeCrdtTest.Unit.ReplicaComponentsTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.Components

  describe "append/5" do
    test "tracks max counter and frontier per origin" do
      cs0 = Components.new()

      cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :d1)
      assert Components.max_counter(cs1, "crdt-1", "replica-a") == 1
      assert Components.frontier(cs1, "crdt-1") == %{"replica-a" => 1}
      assert Components.origins(cs1, "crdt-1") == ["replica-a"]

      cs2 = Components.append(cs1, "crdt-1", "replica-a", 3, :d3)
      assert Components.max_counter(cs2, "crdt-1", "replica-a") == 3
      assert Components.frontier(cs2, "crdt-1") == %{"replica-a" => 3}
      assert Components.since(cs2, "crdt-1", "replica-a", 0) == [{1, :d1}, {3, :d3}]
      assert Components.since(cs2, "crdt-1", "replica-a", 1) == [{3, :d3}]
    end

    test "returns {:error, :duplicate} when counter already exists" do
      cs0 = Components.new()
      cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :d1)

      assert {:error, :duplicate} = Components.append(cs1, "crdt-1", "replica-a", 1, :d1_again)
    end

    test "returns {:error, :non_monotonic} when counter goes backwards" do
      cs0 = Components.new()
      cs1 = Components.append(cs0, "crdt-1", "replica-a", 3, :d3)

      assert {:error, :non_monotonic} = Components.append(cs1, "crdt-1", "replica-a", 2, :d2)
    end
  end

  test "supports multiple origins per CRDT" do
    cs0 = Components.new()

    cs1 = Components.append(cs0, "crdt-1", "replica-a", 1, :a1)
    cs2 = Components.append(cs1, "crdt-1", "replica-b", 2, :b2)

    assert Components.frontier(cs2, "crdt-1") == %{"replica-a" => 1, "replica-b" => 2}
    assert Enum.sort(Components.origins(cs2, "crdt-1")) == ["replica-a", "replica-b"]
  end
end
