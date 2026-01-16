defmodule EdgeCrdt.Protocol.Message.SyncRequest do
  @moduledoc """
  Message sent to request synchronization from another replica.

  Encodes the sync type, include-digest flag, and an encoded digest that
  represents the requester's view.
  """

  alias EdgeCrdt.Replica.Digest

  @behaviour EdgeCrdt.Protocol.Message.Payload

  @typedoc """
  Synchronization request message payload.

  - `:sync_type` - Type of synchronization requested (`:full` or `:delta`).
  - `:digest` - The digest of the requesting replica.
  - `:include_digest?` - Whether to include the current digest of remote replica in the response.
  """
  @enforce_keys [:sync_type, :digest, :include_digest?]
  defstruct [:sync_type, :digest, :include_digest?]

  @type t() :: %__MODULE__{
          sync_type: :full | :delta,
          digest: Digest.t(),
          include_digest?: boolean()
        }

  @impl EdgeCrdt.Protocol.Message.Payload
  def encode(%__MODULE__{sync_type: sync_type, digest: digest, include_digest?: include?}) do
    with {:ok, sync_type_tag} <- encode_sync_type(sync_type),
         {:ok, include_tag} <- encode_include(include?),
         {:ok, digest_bin} <- Digest.encode(digest) do
      digest_len = byte_size(digest_bin)
      {:ok, <<sync_type_tag::8, include_tag::8, digest_len::32, digest_bin::binary>>}
    end
  end

  def encode(other), do: {:error, {:invalid_payload, other}}

  @impl EdgeCrdt.Protocol.Message.Payload
  def decode(<<sync_type_tag::8, include_tag::8, digest_len::32, rest::binary>>) do
    case rest do
      <<digest_bin::binary-size(digest_len), tail::binary>> ->
        with {:ok, sync_type} <- decode_sync_type(sync_type_tag),
             {:ok, include?} <- decode_include(include_tag),
             {:ok, digest} <- Digest.decode(digest_bin),
             :ok <- ensure_trailing(tail) do
          {:ok, %__MODULE__{sync_type: sync_type, digest: digest, include_digest?: include?}}
        end

      _ ->
        {:error, {:invalid_binary, :truncated}}
    end
  end

  def decode(other), do: {:error, {:invalid_binary, other}}

  defp encode_sync_type(:full), do: {:ok, 0}
  defp encode_sync_type(:delta), do: {:ok, 1}
  defp encode_sync_type(other), do: {:error, {:invalid_sync_type, other}}

  defp decode_sync_type(0), do: {:ok, :full}
  defp decode_sync_type(1), do: {:ok, :delta}
  defp decode_sync_type(other), do: {:error, {:invalid_sync_type, other}}

  defp encode_include(true), do: {:ok, 1}
  defp encode_include(false), do: {:ok, 0}
  defp encode_include(other), do: {:error, {:invalid_include_digest, other}}

  defp decode_include(1), do: {:ok, true}
  defp decode_include(0), do: {:ok, false}
  defp decode_include(other), do: {:error, {:invalid_include_digest, other}}

  defp ensure_trailing(<<>>), do: :ok
  defp ensure_trailing(_), do: {:error, {:invalid_binary, :trailing_bytes}}
end
