defmodule EdgeCrdt.Protocol.Message.Header do
  @moduledoc """
  Defines the structure of a message header and its wire encoding.

  The header is a fixed-size binary that precedes every payload and contains
  the payload length in bytes. Any protocol extensions should update
  `@message_types` and keep `@header_size` in sync with the wire format.
  """

  @header_size 12

  defstruct [:protocol_version, :message_type, :flags, :length]

  alias EdgeCrdt.Protocol.Message.{
    DigestRequest,
    DigestResponse,
    SyncRequest,
    SyncResponse
  }

  @message_types %{
    DigestRequest => 1,
    DigestResponse => 2,
    SyncRequest => 3,
    SyncResponse => 4
  }

  @message_types_by_id Map.new(@message_types, fn {type, id} -> {id, type} end)

  @typedoc """
  De-serialized message header.

  - `protocol_version` - The version of the protocol being used.
  - `message_type` - The type of message (e.g., :digest_request, :sync_response).
  - `flags` - A list of flags associated with the message, if any. Represented as atoms.
  - `length` - The length of the message payload in bytes.

  ## Wire Format

  The wire format of the header is as follows:
  - `protocol_version`: 16-bit unsigned integer
  - `message_type`: 16-bit unsigned integer (mapped to module atoms of payload types)
  - `flags`: 32-bit bitfield (each bit represents a flag)
  - `length`: 32-bit unsigned integer
  """
  @type t :: %__MODULE__{
          protocol_version: pos_integer(),
          message_type: DigestRequest | DigestResponse | SyncRequest | SyncResponse,
          flags: [atom()],
          length: non_neg_integer()
        }

  @spec byte_size() :: pos_integer()
  def byte_size, do: @header_size

  @spec encode(t()) :: {:ok, binary()} | {:error, term()}
  def encode(%__MODULE__{
        protocol_version: version,
        message_type: type,
        flags: [],
        length: length
      }) do
    with :ok <- validate_version(version),
         :ok <- validate_length(length),
         {:ok, type_id} <- encode_message_type(type) do
      {:ok, <<version::16, type_id::16, 0::32, length::32>>}
    end
  end

  def encode(%__MODULE__{flags: flags}), do: {:error, {:invalid_flags, flags}}
  def encode(other), do: {:error, {:invalid_header, other}}

  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(<<version::16, type_id::16, flags::32, length::32>>) do
    with :ok <- validate_version(version),
         :ok <- validate_length(length),
         :ok <- validate_flags(flags),
         {:ok, type} <- decode_message_type(type_id) do
      {:ok,
       %__MODULE__{
         protocol_version: version,
         message_type: type,
         flags: [],
         length: length
       }}
    end
  end

  def decode(other), do: {:error, {:invalid_binary, other}}

  defp validate_version(version)
       when is_integer(version) and version > 0 and version <= 0xFFFF,
       do: :ok

  defp validate_version(version), do: {:error, {:invalid_version, version}}

  defp validate_length(length)
       when is_integer(length) and length >= 0 and length <= 0xFFFF_FFFF,
       do: :ok

  defp validate_length(length), do: {:error, {:invalid_length, length}}

  defp validate_flags(0), do: :ok
  defp validate_flags(flags), do: {:error, {:invalid_flags, flags}}

  defp encode_message_type(type) do
    case Map.fetch(@message_types, type) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, {:invalid_message_type, type}}
    end
  end

  defp decode_message_type(id) do
    case Map.fetch(@message_types_by_id, id) do
      {:ok, type} -> {:ok, type}
      :error -> {:error, {:invalid_message_type, id}}
    end
  end
end
