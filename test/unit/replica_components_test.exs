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

  describe "since/2" do
    test "returns deltas for digest origin since counters, defaulting missing CRDTs to 0" do
      cs0 = Components.new()

      cs =
        cs0
        |> Components.append("crdt-1", "replica-a", 1, :a1)
        |> Components.append("crdt-1", "replica-a", 3, :a3)
        |> Components.append("crdt-1", "replica-b", 2, :b2)
        |> Components.append("crdt-2", "replica-a", 2, :a2_crdt2)

      digest = %{"crdt-1" => {"replica-a", 1}}

      bundle = Components.since(cs, digest)

      assert Map.new(Map.fetch!(bundle, "crdt-1")) == %{{"replica-a", 3} => :a3}
      assert Map.new(Map.fetch!(bundle, "crdt-2")) == %{{"replica-a", 2} => :a2_crdt2}
      refute Map.has_key?(bundle, "crdt-3")
    end

    test "returns everything when digest is empty" do
      cs0 = Components.new()

      cs =
        cs0
        |> Components.append("crdt-1", "replica-a", 1, :a1)
        |> Components.append("crdt-1", "replica-b", 2, :b2)
        |> Components.append("crdt-2", "replica-a", 3, :a3)

      bundle = Components.since(cs, %{})

      assert Map.new(Map.fetch!(bundle, "crdt-1")) == %{
               {"replica-a", 1} => :a1,
               {"replica-b", 2} => :b2
             }

      assert Map.new(Map.fetch!(bundle, "crdt-2")) == %{{"replica-a", 3} => :a3}
    end
  end
end
