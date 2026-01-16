defmodule EdgeCrdt.Protocol.Message.DigestRequest do
  @moduledoc """
  Digest request message payload.

  This payload has no fields and encodes to an empty binary.
  """

  @behaviour EdgeCrdt.Protocol.Message.Payload

  defstruct []

  @type t() :: %__MODULE__{}

  def encode(_payload), do: {:ok, <<>>}

  def decode(<<>>), do: {:ok, %__MODULE__{}}
  def decode(_binary), do: {:error, :invalid_binary}
end
