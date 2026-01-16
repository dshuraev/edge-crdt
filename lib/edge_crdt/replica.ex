defmodule EdgeCrdt.Replica do
  @moduledoc """
  A GenServer-based replica that hosts multiple CRDT instances and their causal context.
  """

  alias EdgeCrdt.Crdt
  alias EdgeCrdt.Replica.State

  @typedoc """
  16-byte unique identifier for a replica (UUIDv4).
  """
  @type id :: <<_::16*8>>

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
          {:ok, {Crdt.type(), Crdt.state(), State.meta()}}
          | {:error, {:crdt_not_found, Crdt.id()}}
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

  @spec update_crdt_meta(
          GenServer.server(),
          Crdt.id(),
          State.meta() | (State.meta() -> State.meta())
        ) ::
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

  @impl GenServer
  def handle_info({:edge_crdt_transport, _transport, _peer, _message}, state) do
    # Transport handlers will be added once the sync protocol is defined.
    {:noreply, state}
  end

  defp initial_state(opts) when is_list(opts) do
    case Keyword.get(opts, :state) do
      %State{id: id} = state ->
        if byte_size(id) != 16 do
          {:error, {:invalid_id, id}}
        else
          {:ok, state}
        end

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
end
