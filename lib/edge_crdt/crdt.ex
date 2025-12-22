defmodule EdgeCrdt.Crdt do
  @moduledoc """
  Behaviour for CRDT implementations hosted by `EdgeCrdt.Replica`.
  """

  use Facade

  alias EdgeCrdt.Replica.Context

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
  defapi(zero() :: state())

  @doc """
  Computes the least upper bound for two CRDT states.
  """
  defapi(join(left :: state(), right :: state()) :: result(state()))

  @typep result(t) :: {:ok, t} | {:error, term()}

  @doc """
  Applies a local operation and returns the updated state alongside the minimal
  delta that satisfies the mutation postconditions.
  """
  defapi(
    mutate(state :: state(), op :: op(), dot :: EdgeCrdt.Dot.t()) :: result({state(), delta()})
  )

  @doc """
  Incorporates an incoming delta into the supplied state.
  """
  defapi(apply_delta(state :: state(), delta :: delta(), ctx :: Context.t()) :: result(state()))

  @doc """
  Projects the internal state into a user-facing value.
  """
  defapi(value(state :: state()) :: value())

  @doc """
  Exposes the causal context for the CRDT, if available.
  """
  defapi(context(state :: state()) :: Context.t())

  @doc """
  Returns the wire/version identifier for the CRDT implementation. Used to tag
  events and snapshots so readers can validate compatibility.
  """
  defapi(version() :: 0..0xFFFF)
end
