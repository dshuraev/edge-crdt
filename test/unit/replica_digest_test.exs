defmodule EdgeCrdtTest.Unit.ReplicaDigestTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.Digest

  describe "merge/2" do
    test "keeps max counter per CRDT" do
      a = %{"crdt-1" => {"a", 1}, "crdt-2" => {"a", 3}}
      b = %{"crdt-1" => {"b", 2}, "crdt-3" => {"b", 1}}

      assert Digest.merge(a, b) == %{
               "crdt-1" => {"b", 2},
               "crdt-2" => {"a", 3},
               "crdt-3" => {"b", 1}
             }
    end
  end

  describe "eq?/2" do
    test "compares by counter only (origins ignored)" do
      assert Digest.eq?(%{"crdt-1" => {"a", 2}}, %{"crdt-1" => {"b", 2}})
      refute Digest.eq?(%{"crdt-1" => {"a", 2}}, %{"crdt-1" => {"b", 3}})
    end

    test "treats missing CRDT ids as counter 0" do
      assert Digest.eq?(%{}, %{"crdt-1" => {"a", 0}})
      assert Digest.eq?(%{"crdt-1" => {"a", 0}}, %{})
      refute Digest.eq?(%{}, %{"crdt-1" => {"a", 1}})
    end
  end

  describe "gt?/2" do
    test "is strict and counter-based (origins ignored)" do
      assert Digest.gt?(%{"crdt-1" => {"a", 2}}, %{"crdt-1" => {"b", 1}})
      refute Digest.gt?(%{"crdt-1" => {"a", 1}}, %{"crdt-1" => {"b", 1}})
      refute Digest.gt?(%{"crdt-1" => {"a", 1}}, %{"crdt-1" => {"b", 2}})
    end

    test "fails if b has non-zero counters for CRDTs missing in a" do
      refute Digest.gt?(%{"crdt-1" => {"a", 1}}, %{"crdt-2" => {"b", 1}})
      assert Digest.gt?(%{"crdt-1" => {"a", 1}}, %{"crdt-2" => {"b", 0}})
    end
  end

  describe "since/2" do
    test "returns only CRDTs newer than since" do
      ctx = %{"crdt-1" => {"a", 2}, "crdt-2" => {"a", 0}}
      since = %{"crdt-1" => {"b", 1}, "crdt-2" => {"b", 0}}

      assert Digest.since(ctx, since) == %{"crdt-1" => {"a", 2}}
    end

    test "treats missing CRDT ids in since as counter 0" do
      assert Digest.since(%{"crdt-1" => {"a", 1}}, %{}) == %{"crdt-1" => {"a", 1}}
      assert Digest.since(%{"crdt-1" => {"a", 0}}, %{}) == %{}
    end
  end

  describe "covers_nonzero?/2" do
    test "returns true when all non-zero entries in b exist in a" do
      assert Digest.covers_nonzero?(%{"crdt-1" => {"a", 1}}, %{"crdt-1" => {"b", 0}})
      assert Digest.covers_nonzero?(%{"crdt-1" => {"a", 1}}, %{"crdt-1" => {"b", 2}})
    end

    test "returns false when b has a non-zero CRDT not present in a" do
      refute Digest.covers_nonzero?(%{"crdt-1" => {"a", 1}}, %{"crdt-2" => {"b", 1}})
    end
  end
end
