defmodule EdgeCrdt.Utils.OrdSet do
  @moduledoc false

  @compile {:inline}

  @type t(item) :: list(item)
  @type t :: list()

  @spec new() :: t()
  defdelegate new(), to: :ordsets

  @spec new(Enumerable.t()) :: t()
  def new(list) when is_list(list), do: :ordsets.from_list(list)

  def new(enum) do
    enum |> Enum.to_list() |> new()
  end

  @spec new(Enumerable.t(), (term() -> term())) :: t()
  def new(enum, func) do
    enum
    |> Enum.map(func)
    |> new()
  end

  @spec put(t(), term()) :: t()
  def put(set, item) do
    :ordsets.add_element(item, set)
  end

  @spec size(t()) :: non_neg_integer()
  defdelegate size(set), to: :ordsets, as: :size

  @spec member?(t(), term()) :: boolean()
  def member?(set, item) do
    :ordsets.is_element(item, set)
  end

  @spec empty?(t()) :: boolean()
  defdelegate empty?(set), to: :ordsets, as: :is_empty

  @spec equal?(t(), t()) :: boolean()
  defdelegate equal?(a, b), to: :ordsets, as: :is_equal

  @spec subset?(t(), t()) :: boolean()
  defdelegate subset?(a, b), to: :ordsets, as: :is_subset

  @spec subset?(t(), t()) :: boolean()
  defdelegate difference(a, b), to: :ordsets, as: :subtract

  @spec disjoint?(t(), t()) :: boolean()
  defdelegate disjoint?(a, b), to: :ordsets, as: :is_disjoint

  @spec intersection(list(t())) :: t()
  defdelegate intersection(list), to: :ordsets, as: :intersection
  @spec intersection(t(), t()) :: t()
  defdelegate intersection(a, b), to: :ordsets, as: :intersection

  @spec union(list(t())) :: t()
  defdelegate union(list), to: :ordsets, as: :union
  @spec union(t(), t()) :: t()
  defdelegate union(a, b), to: :ordsets, as: :union
end
