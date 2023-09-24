defmodule CubRepo do
  @moduledoc """
  Creates Repo module.
  """
  @spec select(GenServer.server(), any(), [CubDB.select_option()]) :: Enumerable.t()
  def select(repo, table, options \\ []) do
    CubDB.select(repo, options)
    |> Stream.filter(fn
      {{^table, _key}, _value} -> true
      _otherwise -> false
    end)
    |> Stream.map(fn
      {{_table, key}, value} -> {key, value}
    end)
  end

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    table = Keyword.get(opts, :table, :default)

    quote do
      @repo unquote(repo)
      @table unquote(table)

      unquote(expand_get())
      unquote(expand_put())
      unquote(expand_delete())
      unquote(expand_select())
    end
  end

  defp expand_get do
    quote do
      @spec get(CubDB.key(), CubDB.value()) :: CubDB.value()
      def get(key, default \\ nil) do
        CubDB.get(@repo, {@table, key}, default)
      end
    end
  end

  defp expand_put do
    quote do
      @spec put(CubDB.key(), CubDB.value()) :: :ok
      def put(key, value) do
        CubDB.put(@repo, {@table, key}, value)
      end
    end
  end

  defp expand_delete do
    quote do
      @spec delete(CubDB.key()) :: :ok
      def delete(key) do
        CubDB.delete(@repo, {@table, key})
      end
    end
  end

  defp expand_select do
    quote do
      @spec select([CubDB.select_option()]) :: Enumerable.t()
      def select(options \\ []) do
        CubRepo.select(@repo, @table, options)
      end
    end
  end

  defp expand_key(table) do
    quote do
      {unquote(table), key}
    end
  end
end
