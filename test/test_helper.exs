Logger.configure(level: :info)

defmodule Exandra.TestRepo do
  use Ecto.Repo, otp_app: :exandra, adapter: Exandra
end

defmodule TestConn do
  use Agent

  def start_link(opts) do
    Agent.start_link(fn -> opts end, name: TestConn)
  end

  def value do
    Agent.get(__MODULE__, & &1)
  end
end

Mox.defmock(Exandra.Adapter.Mock, for: Exandra.Adapter.AdapterBehaviour)


Application.put_env(:exandra, :adapter, Exandra.Adapter.Mock)
Application.put_env(:exandra, :child_spec, TestConn)

Application.put_env(:exandra, TestRepo,
  default_consistency: :one,
  keyspace: "test_keyspace",
  log_level: :debug,
  migration_primary_key: [name: :id, type: :binary_id],
  primary_key: [name: :id, type: :binary_id],
  nodes: ["test_node"],
  pool_size: 10,
  protocol_version: :v4
)

defmodule Exandra.AdapterCase do
  use ExUnit.CaseTemplate

  setup do
    conn = Process.whereis(TestConn)
    repo =
      case Exandra.TestRepo.start_link() do
        {:ok, repo} -> repo
        {:error, {:already_started, repo}} -> repo
      end
    
    {:ok, %{conn: conn, repo: repo}}
  end
end

ExUnit.start()
