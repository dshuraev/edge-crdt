defmodule EdgeCrdt.Protocol.Message.SyncResponse do
  @moduledoc """
  Sync response message payload.

  Encodes an optional digest followed by a delta bundle. The digest section
  starts with a flag and length to allow it to be omitted.
  """

  alias EdgeCrdt.Replica.{DeltaBundle, Digest}

  @behaviour EdgeCrdt.Protocol.Message.Payload

  @typedoc """
  Synchronization response message payload.

  - `:bundle` - The delta bundle containing CRDT updates.
  - `:digest` - The current digest of the responding replica,
  only if requested (`EdgeCrdt.Protocol.Message.SyncRequest.include_digest?` was `true`).

  Note: `bundle` may not contain all the necessary updates to bring the requester
  fully up to date with the remote replica's digest.
  """
  @enforce_keys [:bundle]
  defstruct [:bundle, :digest]

  @type t() :: %__MODULE__{
          bundle: DeltaBundle.t(),
          digest: Digest.t() | nil
        }

  @impl EdgeCrdt.Protocol.Message.Payload
  def encode(%__MODULE__{bundle: bundle, digest: digest}) do
    with {:ok, digest_flag, digest_bin} <- encode_digest(digest),
         {:ok, bundle_bin} <- encode_bundle(bundle) do
      digest_len = byte_size(digest_bin)
      {:ok, <<digest_flag::8, digest_len::32, digest_bin::binary, bundle_bin::binary>>}
    end
  end

  def encode(other), do: {:error, {:invalid_payload, other}}

  @impl EdgeCrdt.Protocol.Message.Payload
  def decode(<<digest_flag::8, digest_len::32, rest::binary>>) do
    case rest do
      <<digest_bin::binary-size(digest_len), bundle_bin::binary>> ->
        with {:ok, digest} <- decode_digest(digest_flag, digest_len, digest_bin),
             {:ok, bundle, tail} <- decode_bundle(bundle_bin),
             :ok <- ensure_trailing(tail) do
          {:ok, %__MODULE__{bundle: bundle, digest: digest}}
        end

      _ ->
        {:error, {:invalid_binary, :truncated}}
    end
  end

  def decode(other), do: {:error, {:invalid_binary, other}}

  defp encode_digest(nil), do: {:ok, 0, <<>>}

  defp encode_digest(digest) do
    case Digest.encode(digest) do
      {:ok, bin} -> {:ok, 1, bin}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_digest(0, 0, _bin), do: {:ok, nil}
  defp decode_digest(0, _len, _bin), do: {:error, {:invalid_digest, :unexpected_length}}

  defp decode_digest(1, _len, bin), do: Digest.decode(bin)
  defp decode_digest(flag, _len, _bin), do: {:error, {:invalid_digest_flag, flag}}

  defp encode_bundle(bundle) when is_map(bundle) do
    entries =
      bundle
      |> Map.to_list()
      |> Enum.sort_by(fn {crdt_id, _items} -> crdt_id end)

    with :ok <- validate_bundle_entries(entries),
         {:ok, entries_bin} <- encode_bundle_entries(entries, <<>>) do
      {:ok, <<length(entries)::32, entries_bin::binary>>}
    end
  end

  defp encode_bundle(other), do: {:error, {:invalid_bundle, other}}

  defp validate_bundle_entries(entries) do
    Enum.reduce_while(entries, :ok, fn
      {crdt_id, items}, :ok when is_binary(crdt_id) and is_list(items) and byte_size(crdt_id) <= 0xFFFF ->
        case validate_items(items) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      entry, :ok ->
        {:halt, {:error, {:invalid_entry, entry}}}
    end)
  end

  defp validate_items(items) do
    Enum.reduce_while(items, :ok, fn
      {{origin, counter}, _delta}, :ok
      when is_binary(origin) and byte_size(origin) == 16 and is_integer(counter) and counter >= 0 and
             counter <= 0xFFFF_FFFF_FFFF_FFFF ->
        {:cont, :ok}

      item, :ok ->
        {:halt, {:error, {:invalid_item, item}}}
    end)
  end

  defp encode_bundle_entries([], acc), do: {:ok, acc}

  defp encode_bundle_entries([{crdt_id, items} | rest], acc) do
    crdt_len = byte_size(crdt_id)

    items =
      items
      |> Enum.sort_by(fn {{origin, counter}, _delta} -> {origin, counter} end)

    with {:ok, items_bin} <- encode_items(items, <<>>) do
      entry =
        <<crdt_len::16, crdt_id::binary-size(crdt_len), length(items)::32, items_bin::binary>>

      encode_bundle_entries(rest, <<acc::binary, entry::binary>>)
    end
  end

  defp encode_items([], acc), do: {:ok, acc}

  defp encode_items([{{origin, counter}, delta} | rest], acc) do
    delta_bin = :erlang.term_to_binary(delta)
    delta_len = byte_size(delta_bin)

    item =
      <<origin::binary-size(16), counter::unsigned-64, delta_len::32, delta_bin::binary>>

    encode_items(rest, <<acc::binary, item::binary>>)
  end

  defp decode_bundle(<<count::32, rest::binary>>) do
    decode_bundle_entries(rest, count, %{})
  end

  defp decode_bundle(other), do: {:error, {:invalid_binary, other}}

  defp decode_bundle_entries(rest, 0, acc), do: {:ok, acc, rest}

  defp decode_bundle_entries(<<crdt_len::16, rest::binary>>, count, acc) when count > 0 do
    case rest do
      <<crdt_id::binary-size(crdt_len), items_count::32, tail::binary>> ->
        with :ok <- ensure_unique_crdt(acc, crdt_id),
             {:ok, items, next} <- decode_items(tail, items_count, []) do
          decode_bundle_entries(next, count - 1, Map.put(acc, crdt_id, items))
        end

      _ ->
        {:error, {:invalid_binary, :truncated}}
    end
  end

  defp decode_bundle_entries(_rest, _count, _acc), do: {:error, {:invalid_binary, :truncated}}

  defp decode_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_items(<<origin::binary-size(16), counter::unsigned-64, delta_len::32, rest::binary>>, count, acc)
       when count > 0 do
    case rest do
      <<delta_bin::binary-size(delta_len), tail::binary>> ->
        with {:ok, delta} <- decode_delta(delta_bin) do
          decode_items(tail, count - 1, [{{origin, counter}, delta} | acc])
        end

      _ ->
        {:error, {:invalid_binary, :truncated}}
    end
  end

  defp decode_items(_rest, _count, _acc), do: {:error, {:invalid_binary, :truncated}}

  defp decode_delta(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    error -> {:error, {:invalid_delta, error}}
  end

  defp ensure_unique_crdt(acc, crdt_id) do
    if Map.has_key?(acc, crdt_id) do
      {:error, {:invalid_binary, {:duplicate_crdt, crdt_id}}}
    else
      :ok
    end
  end

  defp ensure_trailing(<<>>), do: :ok
  defp ensure_trailing(_), do: {:error, {:invalid_binary, :trailing_bytes}}
end
