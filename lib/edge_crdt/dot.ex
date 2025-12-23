defmodule EdgeCrdt.Dot do
  @moduledoc """
  A dot represents an event seen by a given actor.

  `{"actor_x", 3}` means that replica `actor_x` has seen event #3.
  """
  alias EdgeCrdt.Replica

  @type counter :: non_neg_integer()
  @type t :: {Replica.id(), counter()}

  @doc """
  Create a new dot. Checks that the dot parameters are formally valid.

  TODO: add tests for success and failure
  """
  @spec new(Replica.id(), pos_integer()) :: t()
  def new(id, clock)
      when is_binary(id) and is_integer(clock) and clock > 0 do
    {id, clock}
  end
end
