defmodule EdgeCrdt.Protocol.Message do
  @moduledoc """
  Message envelope for protocol header, payload, and encoded binary.

  The encoded form is `<<header::binary, payload::binary>>` where the header
  length is fixed (`Header.byte_size/0`) and the payload length is stored in the
  header. Use `encode/2` and `decode/1` to keep the lengths consistent.
  """

  alias EdgeCrdt.Protocol.Message.{Header, Payload}

  defstruct [:header, :payload, :encoded]

  @header_size Header.byte_size()

  @type t :: %__MODULE__{
          header: Header.t(),
          payload: Payload.t(),
          encoded: binary()
        }

  @doc """
  Encodes message header and payload into a binary.
  """
  @spec encode(Header.t(), Payload.t()) :: {:ok, t()} | {:error, term()}
  def encode(header = %Header{message_type: message_type}, payload) do
    with {:ok, payload_bytes} <- Payload.encode(message_type, payload),
         header = %Header{header | length: byte_size(payload_bytes)},
         {:ok, header_bytes} <- Header.encode(header) do
      {:ok,
       %__MODULE__{
         header: header,
         payload: payload,
         encoded: <<header_bytes::binary, payload_bytes::binary>>
       }}
    end
  end

  @doc """
  Decodes a binary into a message header and payload.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(<<header_bytes::binary-size(@header_size), payload_bytes::binary>>) do
    with {:ok, header} <- Header.decode(header_bytes),
         :ok <- ensure_payload_length(header, payload_bytes),
         {:ok, payload} <- Payload.decode(header.message_type, payload_bytes) do
      {:ok,
       %__MODULE__{
         header: header,
         payload: payload,
         encoded: <<header_bytes::binary, payload_bytes::binary>>
       }}
    end
  end

  def decode(other), do: {:error, {:invalid_binary, other}}

  defp ensure_payload_length(%Header{length: length}, payload_bytes)
       when is_integer(length) and length == byte_size(payload_bytes),
       do: :ok

  defp ensure_payload_length(%Header{length: length}, payload_bytes)
       when is_integer(length) and length < byte_size(payload_bytes),
       do: {:error, {:invalid_binary, :trailing_bytes}}

  defp ensure_payload_length(%Header{length: length}, payload_bytes)
       when is_integer(length) and length > byte_size(payload_bytes),
       do: {:error, {:invalid_binary, :truncated}}

  defp ensure_payload_length(_header, _payload_bytes), do: {:error, :invalid_header}
end
