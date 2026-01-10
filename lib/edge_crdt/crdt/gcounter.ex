defmodule EdgeCrdt.Crdt.GCounter do
  @moduledoc """
  Grow-only counter CRDT (G-Counter).
  """

  @behaviour EdgeCrdt.Crdt

  alias EdgeCrdt.Replica
  alias EdgeCrdt.Replica.Context
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

  @impl EdgeCrdt.Crdt
  @spec encode(t()) :: {:ok, binary()} | {:error, term()}
  def encode(state) when is_map(state) do
    entries =
      state
      |> Map.to_list()
      |> Enum.sort_by(fn {replica_id, _} -> replica_id end)

    with :ok <- validate_entries(entries),
         {:ok, entries_bin} <- encode_entries(entries, <<>>) do
      {:ok, <<@vsn::16, length(entries)::32, entries_bin::binary>>}
    end
  end

  def encode(state), do: {:error, {:invalid_state, state}}

  @impl EdgeCrdt.Crdt
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(<<@vsn::16, count::32, rest::binary>>) do
    decode_entries(rest, count, %{})
  end

  def decode(<<version::16, _rest::binary>>), do: {:error, {:unsupported_version, version}}
  def decode(other), do: {:error, {:invalid_encoding, other}}

  defp put_max(state, replica_id, incoming) do
    {:ok, state} = PathMap.update_auto(state, [replica_id], 0, &max(&1, incoming))
    state
  end

  defp validate_entries(entries) do
    Enum.reduce_while(entries, :ok, fn
      {replica_id, counter}, :ok
      when is_binary(replica_id) and is_integer(counter) and counter >= 0 and
             counter <= 0xFFFF_FFFF_FFFF_FFFF ->
        {:cont, :ok}

      entry, :ok ->
        {:halt, {:error, {:invalid_entry, entry}}}
    end)
  end

  defp encode_entries([], acc), do: {:ok, acc}

  defp encode_entries([{replica_id, counter} | rest], acc) do
    id_len = byte_size(replica_id)

    entry =
      <<id_len::16, replica_id::binary-size(id_len), counter::unsigned-64>>

    encode_entries(rest, <<acc::binary, entry::binary>>)
  end

  defp decode_entries(rest, 0, acc) do
    case rest do
      <<>> -> {:ok, acc}
      _ -> {:error, {:invalid_encoding, :trailing_bytes}}
    end
  end

  defp decode_entries(<<id_len::16, rest::binary>>, count, acc)
       when count > 0 do
    case rest do
      <<replica_id::binary-size(id_len), counter::unsigned-64, tail::binary>> ->
        if Map.has_key?(acc, replica_id) do
          {:error, {:invalid_encoding, {:duplicate_replica, replica_id}}}
        else
          decode_entries(tail, count - 1, Map.put(acc, replica_id, counter))
        end

      _ ->
        {:error, {:invalid_encoding, :truncated}}
    end
  end

  defp decode_entries(_rest, _count, _acc), do: {:error, {:invalid_encoding, :truncated}}
end
