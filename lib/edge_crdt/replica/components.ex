defmodule EdgeCrdt.Replica.Components do
  @moduledoc """
  Append-only per-CRDT component log, indexed by origin replica and counter.
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Replica

  @type origin :: Replica.id()
  @type counter :: pos_integer()

  @type crdt_components :: %{
          max_counter: non_neg_integer(),
          deltas: %{counter() => Crdt.delta()}
        }

  @type t :: %__MODULE__{
          by_crdt: %{Crdt.id() => %{origin() => crdt_components()}}
        }

  defstruct by_crdt: %{}

  @spec new() :: t()
  def new, do: %__MODULE__{by_crdt: %{}}

  @spec append(t(), Crdt.id(), origin(), counter(), Crdt.delta()) ::
          t() | {:error, :non_monotonic | :duplicate}
  def append(%__MODULE__{} = cs, crdt_id, origin, counter, delta)
      when is_binary(crdt_id) and is_binary(origin) and is_integer(counter) and counter > 0 do
    crdt_map = Map.get(cs.by_crdt, crdt_id, %{})
    origin_state = Map.get(crdt_map, origin, %{max_counter: 0, deltas: %{}})

    cond do
      Map.has_key?(origin_state.deltas, counter) ->
        {:error, :duplicate}

      counter <= origin_state.max_counter ->
        {:error, :non_monotonic}

      true ->
        new_origin_state = %{
          origin_state
          | max_counter: counter,
            deltas: Map.put(origin_state.deltas, counter, delta)
        }

        new_crdt_map = Map.put(crdt_map, origin, new_origin_state)
        %{cs | by_crdt: Map.put(cs.by_crdt, crdt_id, new_crdt_map)}
    end
  end

  @spec frontier(t(), Crdt.id()) :: %{origin() => non_neg_integer()}
  def frontier(%__MODULE__{} = cs, crdt_id) when is_binary(crdt_id) do
    cs.by_crdt
    |> Map.get(crdt_id, %{})
    |> Enum.into(%{}, fn {origin, %{max_counter: max_counter}} -> {origin, max_counter} end)
  end

  @spec since(t(), Crdt.id(), origin(), non_neg_integer()) :: [{counter(), Crdt.delta()}]
  def since(%__MODULE__{} = cs, crdt_id, origin, counter_exclusive)
      when is_binary(crdt_id) and is_binary(origin) and is_integer(counter_exclusive) and
             counter_exclusive >= 0 do
    case cs.by_crdt |> Map.get(crdt_id, %{}) |> Map.get(origin) do
      %{deltas: deltas} ->
        deltas
        |> Enum.filter(fn {counter, _delta} -> counter > counter_exclusive end)
        |> Enum.sort_by(fn {counter, _delta} -> counter end)

      _ ->
        []
    end
  end

  @spec origins(t(), Crdt.id()) :: [origin()]
  def origins(%__MODULE__{} = cs, crdt_id) when is_binary(crdt_id) do
    cs.by_crdt |> Map.get(crdt_id, %{}) |> Map.keys()
  end

  @spec max_counter(t(), Crdt.id(), origin()) :: non_neg_integer()
  def max_counter(%__MODULE__{} = cs, crdt_id, origin)
      when is_binary(crdt_id) and is_binary(origin) do
    case cs.by_crdt |> Map.get(crdt_id, %{}) |> Map.get(origin) do
      %{max_counter: max_counter} -> max_counter
      _ -> 0
    end
  end
end
