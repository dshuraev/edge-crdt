defmodule EdgeCrdtTest.Unit.DeltaBundleTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica.DeltaBundle

  test "combine/2 merges bundles" do
    a = %{"crdt-1" => [{{"replica-a", 1}, :d1}]}
    b = %{"crdt-2" => [{{"replica-b", 1}, :x1}]}

    assert DeltaBundle.combine(a, b) == %{
             "crdt-1" => [{{"replica-a", 1}, :d1}],
             "crdt-2" => [{{"replica-b", 1}, :x1}]
           }
  end

  test "combine/2 concatenates lists for the same CRDT" do
    a = %{"crdt-1" => [{{"replica-a", 1}, :d1}]}
    b = %{"crdt-1" => [{{"replica-a", 2}, :d2}]}

    assert DeltaBundle.combine(a, b) == %{
             "crdt-1" => [{{"replica-a", 1}, :d1}, {{"replica-a", 2}, :d2}]
           }
  end
end
