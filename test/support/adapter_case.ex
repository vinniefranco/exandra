defmodule Exandra.AdapterCase do
  use ExUnit.CaseTemplate

  import Mox

  using do
    quote do
      import unquote(__MODULE__)
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!

  setup do
    Mox.stub(XandraClusterMock, :child_spec, fn _opts ->
      Supervisor.child_spec({Agent, fn -> :ok end}, [])
    end)

    Mox.stub(XandraClusterMock, :run, fn _cluster, fun ->
      fun.(self())
    end)

    start_link_supervised!(Exandra.TestRepo)

    :ok
  end

  def stub_with_real_modules(_context \\ %{}) do
    stub(XandraClusterMock, :child_spec, &Xandra.Cluster.child_spec/1)
    stub(XandraClusterMock, :execute, &Xandra.Cluster.execute/4)
    stub(XandraClusterMock, :prepare, &Xandra.Cluster.prepare/3)
    stub(XandraClusterMock, :stream_pages!, &Xandra.Cluster.stream_pages!/4)
    stub(XandraClusterMock, :run, &Xandra.Cluster.run/2)

    stub(XandraMock, :start_link, &Xandra.start_link/1)
    stub(XandraMock, :execute, &Xandra.execute/2)
    stub(XandraMock, :execute, &Xandra.execute/4)
    stub(XandraMock, :prepare, &Xandra.prepare/3)

    :ok
  end
end
