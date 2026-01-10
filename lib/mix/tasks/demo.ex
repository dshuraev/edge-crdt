defmodule Mix.Tasks.Demo do
  @moduledoc """
  Minimal demo: two replicas exchange deltas and converge.
  """

  use Mix.Task

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Crdt.GCounter
  alias EdgeCrdt.Replica.State

  @replica_a "000000000000000a"
  @replica_b "000000000000000b"

  @shortdoc "Runs a minimal two-replica convergence demo"
  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start")

    {state_a, state_b} = bootstrap()
    {state_a, state_b} = local_ops(state_a, state_b)

    IO.puts("Replica A value before sync: #{value(state_a, "count")}")
    IO.puts("Replica B value before sync: #{value(state_b, "count")}")

    state_b = sync_from_a(state_a, state_b)

    IO.puts("Replica A value after sync: #{value(state_a, "count")}")
    IO.puts("Replica B value after sync: #{value(state_b, "count")}")
  end

  defp bootstrap do
    state_a = State.new(@replica_a)
    state_b = State.new(@replica_b)

    {:ok, state_a} = State.add_crdt(state_a, "count", GCounter)
    {:ok, state_b} = State.add_crdt(state_b, "count", GCounter)

    {state_a, state_b}
  end

  defp local_ops(state_a, state_b) do
    {:ok, state_a} = State.apply_op(state_a, "count", :inc)
    {:ok, state_a} = State.apply_op(state_a, "count", {:inc, 3})
    {:ok, state_a} = State.apply_op(state_a, "count", :inc)

    {state_a, state_b}
  end

  defp sync_from_a(state_a, state_b) do
    digest_b = State.digest(state_b)
    bundle = State.delta(state_a, digest_b)
    apply_bundle(state_b, bundle)
  end

  defp apply_bundle(state, bundle) do
    Enum.reduce(bundle, state, fn {crdt_id, items}, acc_state ->
      Enum.reduce(items, acc_state, fn {{_replica_id, _counter} = dot, delta}, acc_state2 ->
        {:ok, acc_state2} = State.apply_remote(acc_state2, crdt_id, dot, delta)
        acc_state2
      end)
    end)
  end

  defp value(state, crdt_id) do
    {:ok, {mod, crdt_state, _meta}} = State.fetch_crdt(state, crdt_id)
    Crdt.value(mod, crdt_state)
  end
end
