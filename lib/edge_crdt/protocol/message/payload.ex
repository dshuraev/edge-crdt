defmodule EdgeCrdt.Protocol.Message.Payload do
  @moduledoc """
  Defines behavior for message payloads and dispatch via `Facade`.

  Payload modules implement `encode/1` and `decode/1` and are mapped to message
  type ids in `EdgeCrdt.Protocol.Message.Header`. Add new payload types by
  updating both this union type and the header mapping.
  """

  use Facade

  alias EdgeCrdt.Protocol.Message.{DigestRequest, DigestResponse, SyncRequest, SyncResponse}

  @type t() :: DigestRequest.t() | DigestResponse.t() | SyncRequest.t() | SyncResponse.t()

  defapi(encode(payload :: t()) :: {:ok, binary()} | {:error, term()})
  defapi(decode(binary :: binary()) :: {:ok, t()} | {:error, term()})
end
