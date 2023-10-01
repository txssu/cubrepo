defmodule CubRepo do
  @moduledoc """
  Creates Repo module.
  """

  @type table :: term()
  @type entry :: {{table(), CubDB.key()}, CubDB.value()}

  @type option :: {:repo, term()} | {:key, term()}

  @spec __using__([option]) :: any()
  defmacro __using__(opts) do
    repo = Keyword.get(opts, :repo, quote(do: __MODULE__))
    key = Keyword.get(opts, :key, :id)

    quote do
      @repo unquote(repo)
      @key unquote(key)

      unquote(expand_start_link())

      unquote(expand_get())
      unquote(expand_save())
      unquote(expand_delete_item())
      unquote(expand_delete_by_key())
      unquote(expand_select())
    end
  end

  defp expand_start_link do
    quote do
      @spec start_link([CubDB.option() | {:data_dir, String.t()} | GenServer.option()]) ::
              GenServer.on_start()
      def start_link(options) do
        [name: @repo]
        |> Keyword.merge(options)
        |> CubDB.start_link()
      end
    end
  end

  defp expand_get do
    quote do
      @spec get(CubRepo.table(), CubDB.key(), CubDB.value()) :: CubDB.value()
      def get(table, key, default \\ nil) do
        CubDB.get(@repo, {table, key}, default)
      end
    end
  end

  defp expand_save do
    quote do
      @spec save(CubDB.value()) :: :ok
      def save(item)

      def save(%table{@key => key} = item) do
        CubDB.put(@repo, {table, key}, item)
      end
    end
  end

  defp expand_delete_item do
    quote do
      @spec delete(CubDB.key()) :: :ok
      def delete(item)

      def delete(%table{@key => key}) do
        CubDB.delete(@repo, {table, key})
      end
    end
  end

  defp expand_delete_by_key do
    quote do
      @spec delete(CubRepo.table(), CubDB.key()) :: :ok
      def delete(table, key) do
        CubDB.delete(@repo, {table, key})
      end
    end
  end

  defp expand_select do
    quote do
      @spec select(CubRepo.table(), [CubDB.select_option()]) :: Enumerable.t()
      def select(table, options \\ []) do
        CubRepo.select(@repo, table, options)
      end
    end
  end

  @spec select(GenServer.server(), CubRepo.table(), [CubDB.select_option()]) :: Enumerable.t()
  def select(db, table, options) do
    db
    |> CubDB.select(options)
    |> Stream.filter(&from_table?(&1, table))
    |> Stream.map(&unpack_table_value/1)
  end

  @spec to_entries([CubDB.value()], term()) :: [entry()]
  def to_entries(items, key \\ :id) do
    Enum.map(items, fn %table{^key => key} = item -> {{table, key}, item} end)
  end

  @spec from_table?(entry(), table()) :: boolean
  def from_table?(item, table)

  def from_table?({{table, _key}, _value}, table), do: true
  def from_table?(_item, _table), do: false

  @spec unpack_table_value(entry()) :: CubDB.value()
  def unpack_table_value({_table_key, value}), do: value
end
