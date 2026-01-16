defmodule EdgeCrdt.Protocol.Message.DigestResponse do
  @moduledoc """
  Digest response message payload.

  The payload is an encoded `EdgeCrdt.Replica.Digest` with a deterministic wire
  format.
  """

  @behaviour EdgeCrdt.Protocol.Message.Payload

  alias EdgeCrdt.Replica.Digest

  defstruct [:digest]

  @type t() :: %__MODULE__{digest: Digest.t()}

  def encode(%__MODULE__{digest: digest}), do: Digest.encode(digest)
  def encode(other), do: {:error, {:invalid_payload, other}}
  def decode(binary), do: Digest.decode(binary)
end
