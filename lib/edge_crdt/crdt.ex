defmodule EdgeCrdt.Crdt do
  alias EdgeCrdt.Context

  @typedoc """
  Local ID of the CRDT type within the `EdgeCrdt.Replica`.
  Must be unique withing the context of the owner replica.
  """
  @type id :: binary()

  @typedoc """
  Identifies the type of the CRDT implementation.
  """
  @type type :: module()

  @typedoc """
  Represents the current version of the CRDT. This is used to track changes of
  of the implementation of the underlying CRDT type.
  """
  @type version :: pos_integer()

  @typedoc """
  Opaque state maintained by the CRDT implementation.
  """
  @type state :: term()

  @typedoc """
  Operation describing a local user intent.
  """
  @type op :: term()

  @typedoc """
  Delta element produced by mutations and consumed by replicas.
  """
  @type delta :: state()

  @typedoc """
  External representation of the CRDT state.
  """
  @type value :: term()

  @doc """
  Returns the lattice bottom element for the CRDT.
  """
  @callback zero() :: state()

  @doc """
  Computes the least upper bound for two CRDT states.
  """
  @callback join(state(), state()) :: result(state())

  @typep result(t) :: {:ok, t} | {:error, term()}

  @doc """
  Applies a local operation and returns the updated state alongside the minimal
  delta that satisfies the mutation postconditions.
  """
  @callback mutate(state(), op(), EdgeCrdt.Dot.t()) :: result({state(), delta()})

  @doc """
  Incorporates an incoming delta into the supplied state.
  """
  @callback apply_delta(state(), delta(), Context.t()) :: result(state())

  @doc """
  Projects the internal state into a user-facing value.
  """
  @callback value(state()) :: value()

  @doc """
  Exposes the causal context for the CRDT, if available.
  """
  @callback context(state()) :: Context.t()

  @doc """
  Returns the wire/version identifier for the CRDT implementation. Used to tag
  events and snapshots so readers can validate compatibility.
  """
  @callback version() :: 0..0xFFFF
end
