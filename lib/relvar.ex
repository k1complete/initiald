defmodule Relvar do
  require Logger
  require Qlc

  @type name :: atom
  @type relvar_name :: atom

  defstruct name: nil, types: nil, keys: nil, value: nil, attributes: nil
  @typedoc """
  Relational variable stored in :mnesia table.
  """
  @type t :: %__MODULE__{name: atom, types: map, keys: list, value: map}
  @type keys :: %{atom => any}
  @type attributes :: %{atom => any}
  @type relval :: %{keys => attributes}
  def t(f) do
    :mnesia.transaction(f)
  end
  @spec table(__MODULE__.t) :: Qlc.qlc_handle()
  def table(relvar) when is_atom(relvar) do
    :mnesia.table(relvar)
  end
  def table(relvar) do
    :mnesia.table(relvar.name)
  end
  @spec read(__MODULE__.t, map) :: [tuple]| {:no_exists, atom}
  def read(relvar, key) do
    :mnesia.read({relvar.name, key})
  end
  @spec drop(atom | __MODULE__.t) :: :ok | {:abort, any}
  def drop(relname) when is_atom(relname) do
#    Logger.debug(fn() -> inspect(relname) <> " drop" end)
    :mnesia.delete_table(relname)
  end
  def drop(relvar = %__MODULE__{}) do
    drop(relvar.name)
  end
  @doc """
  create relational variable with keylist and %{attribute_name=>type} set.

  - type checking in attributeset
  """
  @spec create(atom, list, map) :: __MODULE__.t | no_return
  def create(name, keys, attribute_set) when is_list(keys) and is_atom(name) and is_map(attribute_set) do
    v = Map.values(attribute_set)
    {:atomic, s} = t(fn() ->
      qc = Qlc.q("""
            [Type || {_, N, _} <- Q, Type <- Types,
             N =:= Type]
             """, [Q: Reltype.table(), Types: v])
      Qlc.e(qc)
    end)
#    IO.inspect [s: s, v: v]
    case (v -- s) do
      [] ->
        attribute_keys = Map.keys(attribute_set)

        attributes = case [:key | Enum.sort(attribute_keys -- keys)] do
                       [:key] -> [:key, :__dee__]
                       s -> s
                     end
        tabledefs = [type: :set,
                     attributes: attributes,
                     user_properties: [keynames: keys,
                                       types: attribute_set]]
#        IO.inspect [create_var: name, tabledefs: tabledefs]
        {:atomic, :ok} = :mnesia.create_table(name, tabledefs)
        %__MODULE__{name: name, 
                    types: attribute_set, 
                    keys: keys, 
                    attributes: attributes}
      r ->
        :mnesia.abort({:attribute_type_unmatch, name, %{attributes: attribute_set, 
                                                        diff: r}})
    end
  end
  def types(name) when is_atom(name) do
    :mnesia.table_info(name, :user_properties) |>
    Keyword.get(:types)
  end
  @spec to_relvar(atom) :: __MODULE__.t
  def to_relvar(name) when is_atom(name) do
    s = :mnesia.table_info(name, :all)
    u = Keyword.get(s, :user_properties)
    %__MODULE__{name: name, 
                attributes: Keyword.get(s, :attributes),
                types: Keyword.get(u, :types),
                keys: Keyword.get(u, :keynames)}
  end
  @doc """
  validate {k, v} record by type map before writing.
  
  output: invalid {attribute, {type, value}} list.
  """
  @spec valid({map, map}, map) :: list
  def valid({k, v}, types) when is_map(k) and is_map(v) and is_map(types) do
    qc = Qlc.q("""
          [{Name, {TypeName, V}} || 
             Name <- maps:keys(AttributeSet),
             T <- maps:keys(Type),
             {_,TypeName, Definition} <- RelType,
             Name =:= T,
             maps:get(T, Type) =:= TypeName,
             Definition(V = maps:get(Name, AttributeSet)) =:= false]
    """, [AttributeSet: Map.merge(k,v), Type: types, RelType: Reltype.table])
    Qlc.e(qc) 
  end
  @spec to_tuple(atom, {map, map}, list) :: tuple
  @doc """
  traslate key map to value map to record tuple
  """
  def to_tuple(name, {k, v}, alist) when is_atom(name) and is_map(k) and is_map(v) and is_list(alist) do
    attributes = Enum.map(alist, &(Map.get(v, &1)))
    List.to_tuple([name, k | attributes])
  end
  @doc """
  write relational value into relational variable(relvar) as name

  input :  {%{key1: aaa}, %{att1: value1, attr2: value2}}
  translate: {table_name, %{key1: aaa}, value1, value2}
  """
  @type reason :: tuple
  @spec write(atom|__MODULE__.t, {map, map}, list) :: :ok | {reason, atom, map}
  def write(name, {k, v}, alist) when is_atom(name) and is_map(k) and is_map(v) and is_list(alist)  do
    case valid({k, v}, types(name)) do
      [] ->
        to_tuple(name, {k, v}, alist) |> :mnesia.write
      ret ->
        :mnesia.abort({:typecheck_error, name, Enum.into(ret, %{})})
    end
  end
  def write(relvar = %__MODULE__{}, {k, v}) do
    require IEx
    name = relvar.name
    [:key | alist] = relvar.attributes
    case valid({k, v}, relvar.types) do
      [] ->
        s = to_tuple(name, {k, v}, alist)
#        Logger.debug fn() -> inspect [write: s] end
        :mnesia.write(s)
      ret ->
        :mnesia.abort({:typecheck_error, name, Enum.into(ret, %{})})
    end
  end
#  def insert(left, relvar = %__MODULE__) when is_map(left) do
#    Enum.map(left, &(write(relvar, &1)))
#  end
  @spec update(relval, __MODULE__.t) :: relval
  def update(left, %__MODULE__{} = relvar) when is_map(left) do
    Enum.map(left, fn(k, v) -> write(relvar, {k, v}) end)
  end
  def delete(left, %__MODULE__{} = relvar) when is_map(left) do
    Enum.map(left, fn(k, _v) -> delete(relvar, k) end)
  end
  @spec delete(__MODULE__.t, keys) :: :ok | :no_return
  def delete(relvar, key) do
    :mnesia.delete({relvar.name, key})
  end
  
end
defimpl Enumerable, for: Relvar do
  alias Relvar, as: R
  require Logger
  @spec count(R.t) :: {:ok, non_neg_integer} | no_return
  def count(v) do
    case Enum.find(:mnesia.system_info(:tables), &(&1 == v.name)) do
      nil ->
        raise(ArgumentError, "not found relational variable: #{v.name}")
      _ ->
        r = :mnesia.table_info(v.name, :size)
#        IO.inspect [countok: v, r: r]
        {:ok, r}
    end
  end
  def member?(relvar, val) do
    {k, v} = val
    [:key|valuenames] = relvar.attributes
#    Logger.debug fn() -> inspect [member: val] end
#    {:error, __MODULE__}
    case :mnesia.read(relvar.name, k) do
      [] -> {:ok, false}
      [r] -> 
        [t, key|value] = Tuple.to_list(r)
#        IO.inspect [a: {key, value}, a2: {valuenames, value}]
        case (Enum.zip(valuenames, value)|> Map.new()) do
          ^v -> 
            {:ok, true}
          _m  ->
            {:ok, false}
        end
      x -> 
        IO.inspect [x: x]  
        {:error, __MODULE__}
    end
  end
  def reduce(_, {:halt, acc}, _fun), do: {:halted, acc}
  def reduce(v, {:suspend, acc}, fun) do
    {:suspended, acc, &(reduce(v, &1, fun))}
  end
  def reduce(v, {:cont, acc}, fun) do
#    Logger.debug fn() -> inspect([reduce: v]) end
    [:key|attribute_set] = v.attributes
    ret = :mnesia.foldl(fn(r, a) ->
      [_name, key | rest] =  Tuple.to_list(r)
      value = Enum.into(Enum.zip(attribute_set, rest), %{})
      {:cont, r} = fn() ->
#        Logger.debug fn() -> inspect [reduce: {key, value}, acc: a] end
        fun.({key, value}, a)
      end.()
      r
    end, acc, v.name)
    {:done, ret}
  end
end
defimpl Collectable, for: Relvar do
  alias Relvar, as: R
  def into(v) do
    {v, fn 
      (relvar, {:cont, x}) -> 
#        IO.inspect [into: x]
        :ok = R.write(relvar, x)
        relvar
      (relvar, :done) ->
        relvar
      (_relvar, :halt) -> 
        :ok
    end}
  end
end
