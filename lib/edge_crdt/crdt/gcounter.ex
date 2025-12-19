defmodule EdgeCrdt.Crdt.GCounter do
  @moduledoc """
  Grow-only counter CRDT (G-Counter).
  """

  @behaviour EdgeCrdt.Crdt

  alias EdgeCrdt.Context
  alias EdgeCrdt.Replica
  alias PathMap

  @vsn 1

  @opaque t :: %{optional(Replica.id()) => non_neg_integer()}

  @impl EdgeCrdt.Crdt
  def zero, do: %{}

  @impl EdgeCrdt.Crdt
  def join(left, right) when is_map(left) and is_map(right) do
    if :erlang.map_size(left) >= :erlang.map_size(right) do
      apply_delta(left, right, Context.new())
    else
      apply_delta(right, left, Context.new())
    end
  end

  @type op :: :inc | {:inc, pos_integer()}

  @impl EdgeCrdt.Crdt
  def mutate(state, :inc, dot) when is_map(state) do
    mutate(state, {:inc, 1}, dot)
  end

  def mutate(state, {:inc, amount}, {replica_id, _clock})
      when is_map(state) and is_binary(replica_id) and is_integer(amount) and amount > 0 do
    {:ok, state} = PathMap.update_auto(state, [replica_id], 0, fn old -> old + amount end)

    {:ok, new} = PathMap.fetch(state, [replica_id])

    delta = %{replica_id => new}
    {:ok, {state, delta}}
  end

  def mutate(state, op, dot), do: {:error, {:invalid_args, state, op, dot}}

  @impl EdgeCrdt.Crdt
  def apply_delta(state, delta, %Context{}) when is_map(state) and is_map(delta) do
    state =
      Enum.reduce(delta, state, fn {replica_id, incoming}, acc_state ->
        put_max(acc_state, replica_id, incoming)
      end)

    {:ok, state}
  end

  def apply_delta(state, delta, ctx), do: {:error, {:invalid_delta, state, delta, ctx}}

  @impl EdgeCrdt.Crdt
  def value(state) when is_map(state) do
    :maps.fold(fn _replica_id, counter, acc -> acc + counter end, 0, state)
  end

  @impl EdgeCrdt.Crdt
  def context(state) when is_map(state), do: Context.new()

  @impl EdgeCrdt.Crdt
  def version, do: @vsn

  defp put_max(state, replica_id, incoming) do
    {:ok, state} = PathMap.update_auto(state, [replica_id], 0, &max(&1, incoming))
    state
  end
end
