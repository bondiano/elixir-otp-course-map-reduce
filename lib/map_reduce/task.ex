defmodule MapReduce.Task do
  @moduledoc false

  defmodule Job do
    @moduledoc false

    defstruct [:id, :status, :result, :timeout, waiters: []]

    @type t() :: %__MODULE__{
            id: reference(),
            status: :pending | :running | :completed | :failed,
            result: any(),
            timeout: pos_integer(),
            waiters: [pid()]
          }

    def new(opts \\ []) do
      %__MODULE__{
        id: make_ref(),
        status: :pending,
        result: nil,
        timeout: Keyword.get(opts, :timeout, 5000),
        waiters: []
      }
    end

    def execute(job, func) do
      parent = self()

      try do
        result = func.()
        send(parent, {:job_completed, result})

        %__MODULE__{
          job
          | status: :completed,
            result: result
        }
      rescue
        error ->
          send(parent, {:job_failed, error})

          %__MODULE__{
            job
            | status: :failed,
              result: error
          }
      end
    end

    def add_waiter(job, pid) do
      %__MODULE__{job | waiters: [pid | job.waiters]}
    end
  end

  use GenServer

  @type t :: pid()

  @typep state :: Job.t()

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  @spec init(Keyword.t()) :: {:ok, state()}
  def init(opts) do
    {:ok, Job.new(opts)}
  end

  @impl true
  def handle_cast({:execute, func}, state) do
    {:noreply, Job.execute(state, func)}
  end

  @impl true
  def handle_call({:get_result}, from, state) do
    case state.status do
      :completed ->
        {:reply, {:ok, state.result}, state}

      :failed ->
        {:reply, {:error, state.result}, state}

      _ ->
        {:noreply, Job.add_waiter(state, from)}
    end
  end

  @impl true
  def handle_call({:get_status}, _from, state) do
    {:reply, state.status, state}
  end

  @impl true
  def handle_info({:job_completed, result}, state) do
    Enum.each(state.waiters, fn from ->
      GenServer.reply(from, {:ok, result})
    end)

    {:noreply, %{state | status: :completed, result: result, waiters: []}}
  end

  @impl true
  def handle_info({:job_failed, reason}, state) do
    Enum.each(state.waiters, fn from ->
      GenServer.reply(from, {:error, reason})
    end)

    {:noreply, %{state | status: :failed, result: reason, waiters: []}}
  end
end
