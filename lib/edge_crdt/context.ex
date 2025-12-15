defmodule EdgeCrdt.Context do
  @moduledoc """
  `Context` represents a summary of summary of events, both
  local and from remote actors.

  Depending on context `Context` structure can represent on of the following:
  1. Causal summary - all dots observed by a replica.
  2. Context delta - incremental portion of causal summary for a given CRDT delta.

  Currently, it is implemented as a simple set of dots.
  """
  alias EdgeCrdt.Dot
  alias EdgeCrdt.Utils.OrdSet

  @opaque t :: %__MODULE__{}

  defstruct [:sparse]

  @doc """
  Create an empty context.

    iex> EdgeCrdt.Context.new()
    %EdgeCrdt.Context{sparse: %{}}

  """
  @spec new :: t()
  def new, do: %__MODULE__{sparse: %{}}

  @doc """
  Create a new context from an enumerable of dots.

    iex> EdgeCrdt.Context.new([{"a", 1}, {"b", 2}])
    %EdgeCrdt.Context{sparse: %{"a" => [1], "b" => [2]}}
  """
  @spec new(Enumerable.t(Dot.t())) :: t()
  def new(enum) do
    %__MODULE__{
      sparse:
        Enum.reduce(enum, %{}, fn {replica, counter}, acc ->
          Map.update(acc, replica, OrdSet.new([counter]), fn set -> OrdSet.put(set, counter) end)
        end)
    }
  end

  @doc """
  Check if dot is contained in the context.

    iex> EdgeCrdt.Context.new([{"a", 1}]) |> EdgeCrdt.Context.contains?({"a", 1})
    true
  """
  @spec contains?(t(), Dot.t()) :: boolean()
  def contains?(%__MODULE__{sparse: sparse}, {replica, counter}) do
    case Map.fetch(sparse, replica) do
      {:ok, set} -> OrdSet.member?(set, counter)
      :error -> false
    end
  end

  @doc """
  Compare two contexts for equality.

  A context is equal when it has exactly the same dots, grouped per replica.

    iex> a = EdgeCrdt.Context.new([{"a", 1}, {"b", 2}])
    iex> b = EdgeCrdt.Context.new([{"b", 2}, {"a", 1}])
    iex> EdgeCrdt.Context.eq?(a, b)
    true
  """
  @spec eq?(t(), t()) :: boolean()
  def eq?(%__MODULE__{sparse: a}, %__MODULE__{sparse: b}) do
    keys = Map.keys(a) ++ Map.keys(b)

    Enum.all?(keys, fn replica ->
      OrdSet.equal?(
        Map.get(a, replica, OrdSet.new()),
        Map.get(b, replica, OrdSet.new())
      )
    end)
  end

  @doc """
  Return `true` when the first context is a strict subset of the second.

    iex> a = EdgeCrdt.Context.new([{"a", 1}])
    iex> b = EdgeCrdt.Context.new([{"a", 1}, {"a", 2}])
    iex> EdgeCrdt.Context.lt?(a, b)
    true
  """
  @spec lt?(t(), t()) :: boolean()
  def lt?(%__MODULE__{sparse: a}, %__MODULE__{sparse: b}) do
    keys = Map.keys(a) ++ Map.keys(b)

    subset? =
      Enum.all?(keys, fn replica ->
        OrdSet.subset?(
          Map.get(a, replica, OrdSet.new()),
          Map.get(b, replica, OrdSet.new())
        )
      end)

    subset? and
      Enum.any?(keys, fn replica ->
        not OrdSet.equal?(
          Map.get(a, replica, OrdSet.new()),
          Map.get(b, replica, OrdSet.new())
        )
      end)
  end

  @doc """
  Returns `true` if the context is empty for all actors.

    iex> EdgeCrdt.Context.new() |> EdgeCrdt.Context.empty?()
    true
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{sparse: sparse}) do
    if Enum.empty?(sparse) do
      true
    else
      Enum.all?(sparse, &OrdSet.empty?/1)
    end
  end

  def max_for(%__MODULE__{sparse: sparse}, replica_id) do
    Map.get(sparse, replica_id, [0]) |> List.last()
  end

  @doc """
  Add new dot to context.

    iex> EdgeCrdt.Context.new([{"a", 1}]) |> EdgeCrdt.Context.add({"b", 2})
    %EdgeCrdt.Context{sparse: %{"a" => [1], "b" => [2]}}
  """
  @spec add(t(), Dot.t()) :: t()
  def add(ctx = %__MODULE__{sparse: sparse}, {replica, counter}) do
    %__MODULE__{
      ctx
      | sparse:
          Map.update(sparse, replica, OrdSet.new([counter]), fn set ->
            OrdSet.put(set, counter)
          end)
    }
  end

  @doc """
  Merge two contexts together by taking the union of their dots.

    iex> ctx_a = EdgeCrdt.Context.new([{"a", 1}])
    iex> ctx_b = EdgeCrdt.Context.new([{"a", 2}, {"b", 1}])
    iex> EdgeCrdt.Context.join(ctx_a, ctx_b)
    %EdgeCrdt.Context{sparse: %{"a" => [1, 2], "b" => [1]}}
  """
  @spec join(t(), t()) :: t()
  def join(%__MODULE__{sparse: a}, %__MODULE__{sparse: b}) do
    merged =
      Map.merge(a, b, fn _replica, set_a, set_b ->
        OrdSet.union(set_a, set_b)
      end)

    %__MODULE__{sparse: merged}
  end

  @doc """
  Return a context containing dots present in `ctx` but missing in `since`.

    iex> ctx = EdgeCrdt.Context.new([{"a", 1}, {"a", 2}, {"b", 1}])
    iex> since = EdgeCrdt.Context.new([{"a", 1}])
    iex> EdgeCrdt.Context.delta_since(ctx, since)
    %EdgeCrdt.Context{sparse: %{"a" => [2], "b" => [1]}}
  """
  @spec delta_since(t(), t()) :: t()
  def delta_since(%__MODULE__{sparse: ctx}, %__MODULE__{sparse: since}) do
    ctx
    |> Enum.reduce(%{}, fn {replica, set}, acc ->
      diff = OrdSet.difference(set, Map.get(since, replica, OrdSet.new()))

      if OrdSet.empty?(diff) do
        acc
      else
        Map.put(acc, replica, diff)
      end
    end)
    |> then(&%__MODULE__{sparse: &1})
  end
end
