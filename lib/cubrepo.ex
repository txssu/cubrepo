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
      Module.register_attribute(__MODULE__, :associations, accumulate: true)

      @repo unquote(repo)
      @key unquote(key)

      unquote(expand_start_link())

      unquote(expand_get())
      unquote(expand_save())
      unquote(expand_delete_item())
      unquote(expand_delete_by_key())
      unquote(expand_select())

      import CubRepo, only: [deftable: 2]
      require CubRepo

      @before_compile CubRepo
    end
  end

  defmacro deftable(table, module) do
    quote do
      @associations {unquote(table), unquote(module)}
    end
  end

  defmacro __before_compile__(env) do
    associations =
      Module.get_attribute(env.module, :associations)

    associations_functions = Enum.map(associations, &expand_assocation/1)

    quote do
      unquote(associations_functions)

      def table_to_module(table) do
        raise """
        Table "#{table}" not defined. Plase use deftable(#{inspect(table)}, SomeModule) to define table.
        """
      end

      def module_to_table(module) do
        raise """
        Table with module "#{module}" not defined. Plase use deftable(:some_table, #{inspect(module)}) to define table.
        """
      end
    end
  end

  defp expand_assocation({table, module}) do
    quote do
      def table_to_module(unquote(table)), do: unquote(module)
      def module_to_table(unquote(module)), do: unquote(table)
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
      @spec get(module(), CubDB.key(), CubDB.value()) :: CubDB.value()
      def get(module, key, default \\ nil) do
        table = module_to_table(module)
        result = CubDB.get(@repo, {table, key}, default)

        case result do
          nil -> nil
          fields -> struct(module, fields)
        end
      end
    end
  end

  defp expand_save do
    quote do
      @spec save(CubDB.value()) :: :ok
      def save(item)

      def save(%module{@key => key} = item) do
        table = module_to_table(module)
        CubDB.put(@repo, {table, key}, Map.from_struct(item))
      end
    end
  end

  defp expand_delete_item do
    quote do
      @spec delete(CubRepo.value()) :: :ok
      def delete(item)

      def delete(%module{@key => key}) do
        table = module_to_table(module)
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
      @spec select(module(), [CubDB.select_option()]) :: Enumerable.t()
      def select(module, options \\ []) do
        table = module_to_table(module)

        CubRepo.select(@repo, table, options)
        |> Stream.map(&struct(module, &1))
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
