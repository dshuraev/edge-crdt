defmodule EdgeCrdt.Replica.State do
  @moduledoc """
  Internal state and pure update operations for `EdgeCrdt.Replica`.
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Dot
  alias EdgeCrdt.Replica
  alias EdgeCrdt.Replica.Components
  alias EdgeCrdt.Replica.Context
  alias EdgeCrdt.Replica.DeltaBundle

  @typedoc """
  Internal state of replica.

  - `:id` - unique identifier for the replica
  - `:crdts` - map of CRDT instances by their ID
  - `:ctx` - global causal context
  - `:components` - nested map of components (deltas) produced by every update,
    keyed by CRDT ID and replica ID that originated the update.
  - `:policy` - map of policy flags (max log size, GC policy, etc.)
  - `:transport_pid` - optional PID of transport process used for inter-replica communication
  """
  defstruct [:id, :crdts, :ctx, :components, :policy, :transport_pid]

  @type meta :: map()
  @type policy :: map()

  @type t :: %__MODULE__{
          id: Replica.id(),
          crdts: %{Crdt.id() => {Crdt.type(), Crdt.state(), meta()}},
          ctx: Context.t(),
          components: Components.t(),
          policy: policy(),
          transport_pid: pid() | nil
        }

  @doc """
  Create an empty replica with the given ID and optional policy flags.
  """
  @spec new(Replica.id(), Keyword.t()) :: t() | {:error, {:invalid_id, Replica.id()}}
  def new(id, opts \\ [])

  def new(id, opts) when is_binary(id) do
    %__MODULE__{
      id: id,
      crdts: %{},
      ctx: Context.new(),
      components: Components.new(),
      policy: Map.new(opts),
      transport_pid: Keyword.get(opts, :transport_pid, nil)
    }
  end

  def new(id, _opts), do: {:error, {:invalid_id, id}}

  @doc """
  Add new CRDT instance of `type` with `id` to replica.

  `type` must be a module implementing `EdgeCrdt.Crdt` behaviour.

  Keyword options:
   * `overwrite`: If true, overwrite existing CRDT instance. Defaults to false.
   * `initial_state`: Initial state of the CRDT instance. Defaults to the `EdgeCrdt.Crdt.zero(type)` element of the CRDT type.
  """
  @spec add_crdt(t(), Crdt.id(), Crdt.type(), Keyword.t()) ::
          {:ok, t()} | {:error, :already_exists}
  def add_crdt(state = %__MODULE__{crdts: crdts}, id, type, opts \\ []) do
    overwrite = Keyword.get(opts, :overwrite, false)
    meta = %{}

    if not overwrite and Map.has_key?(crdts, id) do
      {:error, :already_exists}
    else
      case Crdt.validate(type) do
        :ok ->
          crdt = Keyword.get(opts, :initial_state, Crdt.zero(type))
          new_crdts = Map.put(crdts, id, {type, crdt, meta})
          {:ok, %__MODULE__{state | crdts: new_crdts}}

        {:error, missing} ->
          {:error, {:implementation_missing, type, missing}}
      end
    end
  end

  @doc """
  Get CRDT type, state, and metadata by id.
  """
  @spec fetch_crdt(t(), Crdt.id()) ::
          {:ok, {Crdt.type(), Crdt.state(), meta()}} | {:error, {:crdt_not_found, Crdt.id()}}
  def fetch_crdt(%__MODULE__{crdts: crdts}, id) do
    case Map.fetch(crdts, id) do
      {:ok, val} -> {:ok, val}
      :error -> {:error, {:crdt_not_found, id}}
    end
  end

  @spec list_crdts(t()) :: [{Crdt.id(), Crdt.type(), meta()}]
  def list_crdts(%__MODULE__{crdts: crdts}) do
    Enum.map(crdts, fn {id, {mod, _, meta}} -> {id, mod, meta} end)
  end

  @doc """
  Ensure a CRDT exists, creating it if missing.
  """
  @spec ensure_crdt(t(), Crdt.id(), Crdt.type(), Keyword.t()) ::
          {:ok, t()} | {:error, term()}
  def ensure_crdt(state = %__MODULE__{}, crdt_id, crdt_type, opts \\ []) do
    case fetch_crdt(state, crdt_id) do
      {:ok, _} ->
        {:ok, state}

      {:error, {:crdt_not_found, _}} ->
        case add_crdt(state, crdt_id, crdt_type, opts) do
          {:ok, new_state} -> {:ok, new_state}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Update CRDT metadata either by applying a function or replacing it.
  """
  @spec update_crdt_meta(t(), Crdt.id(), meta() | (meta() -> meta())) ::
          {:ok, t()} | :error | {:error, term()}
  def update_crdt_meta(state = %__MODULE__{}, crdt_id, new_meta) when is_map(new_meta) do
    case fetch_crdt(state, crdt_id) do
      {:ok, {mod, crdt_state, _meta}} ->
        new_crdts = Map.put(state.crdts, crdt_id, {mod, crdt_state, new_meta})
        {:ok, %__MODULE__{state | crdts: new_crdts}}

      e ->
        e
    end
  end

  def update_crdt_meta(state = %__MODULE__{}, crdt_id, updater) when is_function(updater, 1) do
    case fetch_crdt(state, crdt_id) do
      {:ok, {mod, crdt_state, meta}} ->
        case updater.(meta) do
          updated_meta when is_map(updated_meta) ->
            new_crdts = Map.put(state.crdts, crdt_id, {mod, crdt_state, updated_meta})
            {:ok, %__MODULE__{state | crdts: new_crdts}}

          other ->
            {:error, {:invalid_meta, other}}
        end

      e ->
        e
    end
  end

  def update_crdt_meta(%__MODULE__{}, _crdt_id, bad), do: {:error, {:invalid_meta, bad}}

  @doc """
  Apply *local* operation.

  NOTE: the operation is applied *before* new dot is added to replica's context.
  If applying operation fails, the dot is **not** persisted.
  """
  @spec apply_op(t(), Crdt.id(), Crdt.op()) :: {:ok, t()} | {:error, term()} | :error
  def apply_op(state = %__MODULE__{}, crdt_id, op) do
    # mint a fresh dot for current replica
    dot = next_dot(state)

    with {:ok, {crdt_mod, crdt_state, meta}} <- fetch_crdt(state, crdt_id),
         {:ok, {crdt_state, delta}} <- Crdt.mutate(crdt_mod, crdt_state, op, dot),
         {:ok, state} <- put_state(state, crdt_id, {crdt_mod, crdt_state, meta}),
         {:ok, state} <- put_component(state, crdt_id, dot, delta) do
      # persist the dot - update clock for current replica
      new_context = Context.add(state.ctx, dot)
      {:ok, %{state | ctx: new_context}}
    end
  end

  @spec next_dot(t()) :: Dot.t()
  defp next_dot(%__MODULE__{id: id, ctx: ctx}) do
    {id, Context.max_for(ctx, id) + 1}
  end

  defp put_state(state = %__MODULE__{}, crdt_id, {crdt_mod, crdt_state, meta}) do
    PathMap.put_auto(state, [:crdts, crdt_id], {crdt_mod, crdt_state, meta})
  end

  # Store a component (delta tagged with its dot clock) keyed by CRDT and source replica.
  @spec put_component(t(), Crdt.id(), Dot.t(), Crdt.delta()) ::
          {:ok, t()} | {:error, term()}
  defp put_component(state = %__MODULE__{}, crdt_id, {replica_id, clock}, delta) do
    case Components.append(state.components, crdt_id, replica_id, clock, delta) do
      %Components{} = components ->
        {:ok, %{state | components: components}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Apply a single remote delta for the given CRDT and replica.

  NOTE: the delta is applied *before* new dot is added to replica's context.
  If applying delta fails, the dot is **not** persisted.
  """
  @spec apply_remote(t(), Crdt.id(), Dot.t(), Crdt.delta()) ::
          {:ok, t()} | {:error, term()}
  def apply_remote(
        state = %__MODULE__{},
        crdt_id,
        {replica_id, clock} = dot,
        delta
      )
      when is_binary(replica_id) and is_integer(clock) and clock > 0 do
    if Context.contains?(state.ctx, dot) do
      {:ok, state}
    else
      with {:ok, {crdt_mod, crdt_state, meta}} <- fetch_crdt(state, crdt_id),
           {:ok, crdt_state} <- Crdt.apply_delta(crdt_mod, crdt_state, delta, state.ctx),
           {:ok, state} <- put_state(state, crdt_id, {crdt_mod, crdt_state, meta}),
           {:ok, state} <- put_component(state, crdt_id, dot, delta) do
        ctx = Context.add(state.ctx, dot)
        {:ok, %{state | ctx: ctx}}
      end
    end
  end

  def apply_remote(%__MODULE__{}, _crdt_id, bad_dot, _delta),
    do: {:error, {:invalid_dot, bad_dot}}

  @doc """
  Compute a per-CRDT digest of locally-originated updates.

  This summary can be used for anti-entropy to request missing deltas from this
  replica for each CRDT.
  """
  @spec digest(t()) :: Replica.Digest.t()
  def digest(%__MODULE__{id: replica_id, crdts: crdts, ctx: ctx})
      when is_binary(replica_id) and is_map(crdts) do
    max_counter = Context.max_for(ctx, replica_id)

    Enum.reduce(crdts, %{}, fn {crdt_id, _val}, acc ->
      Map.put(acc, crdt_id, {replica_id, max_counter})
    end)
  end

  @doc """
  Extract deltas originated by this replica since the given digest.
  """
  @spec delta(t(), Replica.Digest.t()) :: DeltaBundle.t()
  def delta(
        %__MODULE__{id: replica_id, crdts: crdts, components: %Components{by_crdt: by_crdt}},
        since_digest
      )
      when is_binary(replica_id) and is_map(crdts) and is_map(since_digest) do
    Enum.reduce(crdts, %{}, fn {crdt_id, _val}, acc ->
      counter_exclusive =
        case Map.fetch(since_digest, crdt_id) do
          {:ok, {_origin, counter}} when is_integer(counter) and counter >= 0 -> counter
          _ -> 0
        end

      origin_components =
        by_crdt
        |> Map.get(crdt_id, %{})
        |> Map.get(replica_id, %{})

      items =
        for {counter, delta} <- origin_components, counter > counter_exclusive do
          {{replica_id, counter}, delta}
        end

      case items do
        [] -> acc
        _ -> Map.put(acc, crdt_id, items)
      end
    end)
  end
end
