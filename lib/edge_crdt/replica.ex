defmodule EdgeCrdt.Replica do
  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Context
  alias __MODULE__.State

  @type id :: binary()

  use GenServer

  @spec start_link(Keyword.t(), Keyword.t()) :: GenServer.on_start()
  def start_link(opts, genserver_opts \\ []) when is_list(opts) and is_list(genserver_opts) do
    with {:ok, state} <- initial_state(opts) do
      name =
        case Keyword.fetch(opts, :name) do
          {:ok, name} ->
            name

          :error ->
            case Keyword.fetch(opts, :registry) do
              {:ok, registry} when is_atom(registry) -> via(registry, state.id)
              _ -> nil
            end
        end

      genserver_opts =
        if is_nil(name) do
          genserver_opts
        else
          Keyword.put_new(genserver_opts, :name, name)
        end

      GenServer.start_link(__MODULE__, state, genserver_opts)
    end
  end

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    replica_id =
      case Keyword.get(opts, :state) do
        %{__struct__: EdgeCrdt.Replica.State, id: id} -> id
        _ -> Keyword.get(opts, :id, __MODULE__)
      end

    %{
      id: {__MODULE__, replica_id},
      start: {__MODULE__, :start_link, [opts, []]}
    }
  end

  @spec via(atom(), id()) :: {:via, Registry, {atom(), id()}}
  def via(registry, id) when is_atom(registry) and is_binary(id) do
    {:via, Registry, {registry, id}}
  end

  @spec whereis(atom(), id()) :: pid() | nil
  def whereis(registry, id) when is_atom(registry) and is_binary(id) do
    case Registry.lookup(registry, id) do
      [{pid, _val} | _] -> pid
      [] -> nil
    end
  end

  @spec add_crdt(GenServer.server(), Crdt.id(), Crdt.type(), Keyword.t()) ::
          :ok | {:error, term()}
  def add_crdt(server, crdt_id, crdt_type, opts \\ []) do
    GenServer.call(server, {:add_crdt, crdt_id, crdt_type, opts})
  end

  @spec fetch_crdt(GenServer.server(), Crdt.id()) ::
          {:ok, {Crdt.type(), Crdt.state(), State.meta()}} | {:error, {:crdt_not_found, Crdt.id()}}
  def fetch_crdt(server, crdt_id) do
    GenServer.call(server, {:fetch_crdt, crdt_id})
  end

  @spec list_crdts(GenServer.server()) :: [{Crdt.id(), Crdt.type(), State.meta()}]
  def list_crdts(server) do
    GenServer.call(server, :list_crdts)
  end

  @spec ensure_crdt(GenServer.server(), Crdt.id(), Crdt.type(), Keyword.t()) ::
          :ok | {:error, term()}
  def ensure_crdt(server, crdt_id, crdt_type, opts \\ []) do
    GenServer.call(server, {:ensure_crdt, crdt_id, crdt_type, opts})
  end

  @spec update_crdt_meta(GenServer.server(), Crdt.id(), State.meta() | (State.meta() -> State.meta())) ::
          :ok | :error | {:error, term()}
  def update_crdt_meta(server, crdt_id, new_meta) do
    GenServer.call(server, {:update_crdt_meta, crdt_id, new_meta})
  end

  @spec apply_op(GenServer.server(), Crdt.id(), Crdt.op()) :: :ok | {:error, term()} | :error
  def apply_op(server, crdt_id, op) do
    GenServer.call(server, {:apply_op, crdt_id, op})
  end

  @spec apply_remote(GenServer.server(), Crdt.id(), EdgeCrdt.Dot.t(), Crdt.delta()) ::
          :ok | {:error, term()}
  def apply_remote(server, crdt_id, dot, delta) do
    GenServer.call(server, {:apply_remote, crdt_id, dot, delta})
  end

  @impl GenServer
  def init(state) do
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:add_crdt, crdt_id, crdt_type, opts}, _from, state) do
    case State.add_crdt(state, crdt_id, crdt_type, opts) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      other -> {:reply, other, state}
    end
  end

  def handle_call({:fetch_crdt, crdt_id}, _from, state) do
    {:reply, State.fetch_crdt(state, crdt_id), state}
  end

  def handle_call(:list_crdts, _from, state) do
    {:reply, State.list_crdts(state), state}
  end

  def handle_call({:ensure_crdt, crdt_id, crdt_type, opts}, _from, state) do
    case State.ensure_crdt(state, crdt_id, crdt_type, opts) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      other -> {:reply, other, state}
    end
  end

  def handle_call({:update_crdt_meta, crdt_id, new_meta}, _from, state) do
    case State.update_crdt_meta(state, crdt_id, new_meta) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      other -> {:reply, other, state}
    end
  end

  def handle_call({:apply_op, crdt_id, op}, _from, state) do
    case State.apply_op(state, crdt_id, op) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      other -> {:reply, other, state}
    end
  end

  def handle_call({:apply_remote, crdt_id, dot, delta}, _from, state) do
    case State.apply_remote(state, crdt_id, dot, delta) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      other -> {:reply, other, state}
    end
  end

  defp initial_state(opts) when is_list(opts) do
    case Keyword.get(opts, :state) do
      %{__struct__: EdgeCrdt.Replica.State} = state ->
        {:ok, state}

      nil ->
        with {:ok, id} <- Keyword.fetch(opts, :id),
             true <- is_binary(id) do
          policy_opts = Keyword.drop(opts, [:id, :name, :registry, :state])

          case State.new(id, policy_opts) do
            %{__struct__: EdgeCrdt.Replica.State} = state -> {:ok, state}
            {:error, reason} -> {:error, reason}
          end
        else
          :error -> {:error, :missing_id}
          false -> {:error, {:invalid_id, Keyword.get(opts, :id)}}
        end

      other ->
        {:error, {:invalid_state, other}}
    end
  end

  defmodule State do
    alias EdgeCrdt.Dot
    alias EdgeCrdt.Utils.OrdSet
    alias EdgeCrdt.Replica

    @typedoc """
    Internal state of replica.

    - `:id` - unique identifier for the replica
    - `:crdts` - map of CRDT instances by their ID
    - `:ctx` - global causal context
    - `:components` - nested map of components (deltas) produced by every update,
      keyed by CRDT ID and replica ID that originated the update.
    - `:policy` - map of policy flags (max log size, GC policy, etc.)
    """
    defstruct [:id, :crdts, :ctx, :components, :policy]

    @type meta :: map()
    @type policy :: map()

    @type t :: %__MODULE__{
            id: Replica.id(),
            crdts: %{Crdt.id() => {Crdt.type(), Crdt.state(), meta()}},
            ctx: Context.t(),
            components: %{
              Crdt.id() => %{Replica.id() => OrdSet.t({counter :: pos_integer(), Crdt.delta()})}
            },
            policy: policy()
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
        components: %{},
        policy: Map.new(opts)
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
        if function_exported?(type, :zero, 0) do
          crdt = Keyword.get(opts, :initial_state, Crdt.zero(type))
          new_crdts = Map.put(crdts, id, {type, crdt, meta})
          {:ok, %__MODULE__{state | crdts: new_crdts}}
        else
          {:error, {:implementation_missing, type, [{:zero, 0}]}}
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

        e -> e
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

        e -> e
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

    defp put_state(%__MODULE__{} = state, crdt_id, {crdt_mod, crdt_state, meta}) do
      PathMap.put_auto(state, [:crdts, crdt_id], {crdt_mod, crdt_state, meta})
    end

    # Store a component (delta tagged with its dot clock) keyed by CRDT and source replica.
    @spec put_component(t(), Crdt.id(), Dot.t(), Crdt.delta()) ::
            {:ok, t()} | {:error, term()}
    defp put_component(%__MODULE__{} = state, crdt_id, {replica_id, clock}, delta) do
      component = {clock, delta}

      PathMap.update_auto(
        state,
        [:components, crdt_id, replica_id],
        OrdSet.new([component]),
        fn set -> OrdSet.put(set, component) end
      )
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
  end
end
