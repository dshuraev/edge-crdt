defmodule EdgeCrdt.Replica.DeltaBundle do
  @moduledoc """
  A bundle of deltas for multiple CRDTs.
  """
  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Dot

  @type t :: %{Crdt.id() => [{Dot.t(), Crdt.delta()}]}

  @spec combine(t(), t()) :: t()
  def combine(a, b) when is_map(a) and is_map(b) do
    Enum.reduce(b, a, fn {crdt_id, items_b}, acc ->
      Map.update(acc, crdt_id, items_b, fn items_a -> items_a ++ items_b end)
    end)
  end
end
