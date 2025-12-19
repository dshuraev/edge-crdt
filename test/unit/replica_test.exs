defmodule EdgeCrdtTest.Unit.ReplicaTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Replica

  defmodule TestCrdt do
    def zero, do: :zero
  end

  test "start_link/2 registers via Registry and exposes State APIs" do
    registry = :"replica_registry_#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    replica_id = "replica-a"
    {:ok, pid} = start_supervised({Replica, [id: replica_id, registry: registry]})
    assert pid == Replica.whereis(registry, replica_id)

    server = Replica.via(registry, replica_id)
    assert :ok == Replica.ensure_crdt(server, "crdt-1", TestCrdt)

    assert {:ok, {TestCrdt, :zero, %{}}} = Replica.fetch_crdt(server, "crdt-1")
    assert [{"crdt-1", TestCrdt, %{}}] = Replica.list_crdts(server)
  end
end
