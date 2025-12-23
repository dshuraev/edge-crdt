defmodule EdgeCrdt.Replica.Digest do
  @moduledoc """
  Per-CRDT digest of a replica.

  A digest summarizes *how much* of each CRDT a replica has seen, using the
  maximum dot counter per CRDT (the dot origin is treated as informational).

  Missing CRDT entries are treated as `{_origin, 0}` in comparisons, which
  makes digests convenient as anti-entropy cursors: an unknown CRDT implies
  "send me everything".
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Dot

  @type t :: %{Crdt.id() => Dot.t()}

  @doc """
  Merge two digests by taking the maximum counter per CRDT.

  Origins are ignored for ordering; the dot with the highest counter is kept.
  """
  @spec merge(t(), t()) :: t()
  def merge(a, b) when is_map(a) and is_map(b) do
    Map.merge(a, b, fn _crdt_id, {_origin_a, counter_a} = dot_a, {_origin_b, counter_b} = dot_b ->
      if counter_a >= counter_b, do: dot_a, else: dot_b
    end)
  end

  @doc """
  Returns `true` when two digests have the same counters for all CRDT ids.

  Missing ids are treated as counter `0`.
  """
  @spec eq(t(), t()) :: boolean()
  def eq(a, b) when is_map(a) and is_map(b) do
    Enum.all?(a, fn {crdt_id, {_origin_a, counter_a}} ->
      counter_a == counter_for(b, crdt_id)
    end) and
      Enum.all?(b, fn {crdt_id, {_origin_b, counter_b}} ->
        counter_b == counter_for(a, crdt_id)
      end)
  end

  @doc "Predicate variant of `eq/2`."
  @spec eq?(t(), t()) :: boolean()
  def eq?(a, b), do: eq(a, b)

  @doc """
  Returns `true` when `a` is strictly greater than `b`.

  "Greater than" means: for every CRDT id, `a[id] >= b[id]` (treating missing
  ids as counter `0`), and for at least one id, `a[id] > b[id]`.
  """
  @spec gt(t(), t()) :: boolean()
  def gt(a, b) when is_map(a) and is_map(b) do
    a_strict_or_error =
      Enum.reduce_while(a, false, fn {crdt_id, {_origin_a, counter_a}}, strict? ->
        counter_b = counter_for(b, crdt_id)

        cond do
          counter_a < counter_b -> {:halt, :error}
          counter_a > counter_b -> {:cont, true}
          true -> {:cont, strict?}
        end
      end)

    case a_strict_or_error do
      :error ->
        false

      strict? ->
        strict? and covers_nonzero?(a, b)
    end
  end

  @doc "Predicate variant of `gt/2`."
  @spec gt?(t(), t()) :: boolean()
  def gt?(a, b), do: gt(a, b)

  @doc """
  Returns the portion of `ctx` that is "newer" than `since`.

  The result includes only CRDT ids whose counter in `ctx` is greater than the
  corresponding counter in `since` (missing ids in `since` are treated as `0`).
  """
  @spec since(t(), t()) :: t()
  def since(ctx, since) when is_map(ctx) and is_map(since) do
    Enum.reduce(ctx, %{}, fn {crdt_id, {_origin_ctx, counter_ctx} = dot_ctx}, acc ->
      if counter_ctx > counter_for(since, crdt_id) do
        Map.put(acc, crdt_id, dot_ctx)
      else
        acc
      end
    end)
  end

  defp counter_for(digest, crdt_id) when is_map(digest) do
    case Map.fetch(digest, crdt_id) do
      {:ok, {_origin, counter}} -> counter
      :error -> 0
    end
  end

  @doc """
  Returns `true` if every CRDT with a non-zero counter in `b` is present in `a`.

  This is useful when treating missing digest entries as implicitly `{origin, 0}`.
  """
  @spec covers_nonzero?(t(), t()) :: boolean()
  def covers_nonzero?(a, b) when is_map(a) and is_map(b) do
    Enum.reduce_while(b, true, fn {crdt_id, {_origin_b, counter_b}}, _acc ->
      cond do
        Map.has_key?(a, crdt_id) -> {:cont, true}
        counter_b == 0 -> {:cont, true}
        true -> {:halt, false}
      end
    end)
  end
end
