defmodule EdgeCrdt.Replica.Digest do
  @moduledoc """
  Digest of a replica.
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Dot

  @type t :: %{Crdt.id() => Dot.t()}
end
