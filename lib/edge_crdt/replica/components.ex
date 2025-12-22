defmodule EdgeCrdt.Replica.Components do
  @moduledoc """
  Append-only per-CRDT component log, indexed by origin replica and counter.
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Replica
  alias EdgeCrdt.Replica.DeltaBundle

  @type origin :: Replica.id()
  @type counter :: pos_integer()

  @type crdt_components :: %{counter() => Crdt.delta()}

  @type t :: %__MODULE__{
          by_crdt: %{Crdt.id() => %{origin() => crdt_components()}}
        }

  defstruct by_crdt: %{}

  @spec new() :: t()
  def new, do: %__MODULE__{by_crdt: %{}}

  @spec append(t(), Crdt.id(), origin(), counter(), Crdt.delta()) :: t() | {:error, :duplicate}
  def append(%__MODULE__{} = cs, crdt_id, origin, counter, delta)
      when is_binary(crdt_id) and is_binary(origin) and is_integer(counter) and counter > 0 do
    crdt_map = Map.get(cs.by_crdt, crdt_id, %{})
    origin_deltas = Map.get(crdt_map, origin, %{})

    if Map.has_key?(origin_deltas, counter) do
      {:error, :duplicate}
    else
      new_origin_deltas = Map.put(origin_deltas, counter, delta)
      new_crdt_map = Map.put(crdt_map, origin, new_origin_deltas)
      %{cs | by_crdt: Map.put(cs.by_crdt, crdt_id, new_crdt_map)}
    end
  end

  @spec since(t(), Crdt.id(), origin(), non_neg_integer()) :: DeltaBundle.t()
  def since(%__MODULE__{} = cs, crdt_id, origin, counter_exclusive)
      when is_binary(crdt_id) and is_binary(origin) and is_integer(counter_exclusive) and
             counter_exclusive >= 0 do
    deltas =
      cs.by_crdt
      |> Map.get(crdt_id, %{})
      |> Map.get(origin, %{})

    items =
      for {counter, delta} <- deltas, counter > counter_exclusive do
        {{origin, counter}, delta}
      end

    if items == [] do
      %{}
    else
      %{crdt_id => items}
    end
  end

  @spec origins(t(), Crdt.id()) :: [origin()]
  def origins(%__MODULE__{} = cs, crdt_id) when is_binary(crdt_id) do
    cs.by_crdt |> Map.get(crdt_id, %{}) |> Map.keys()
  end
end
