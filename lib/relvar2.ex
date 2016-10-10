defmodule Relvar2 do
  require Logger
  require Qlc
  alias Relval2, as: L

  defstruct name: nil, types: nil, keys: nil, attributes: nil, constraints: nil

  def add_index(%__MODULE__{name: v} = _relvar, att) do
    :mnesia.add_table_index(v, att)
  end

  def del_index(%__MODULE__{name: v} = _relvar, att) do
    :mnesia.del_table_index(v, att)
  end
  defmacro sel!(type, value) do
    quote bind_quoted: [type: type, value: value] do
      [{_, _, v, cast}] = Reltype.read(type)
      c = case cast do
        nil -> value
        cast -> cast.(value)
      end
      case v.(c) do 
        true ->
          c
        false ->
          raise ArgumentError, "invalid type: #{c} is not #{type}"
      end
    end
  end
  @type name :: atom
  @type relvar_name :: atom

  @typedoc """
  Relational variable stored in :mnesia table.
  """
  @type t :: %__MODULE__{name: atom, types: list, keys: list, constraints: list }
  @type keys :: tuple
  @type attributes :: tuple
  @type relval :: any
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
  def record_to_tuple(e) do
    t = Tuple.delete_at(e, 0)
    Tuple.delete_at(t, 0)
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
  @spec create(atom, list, [{atom, any}]) :: __MODULE__.t | no_return
  def create(name, keys, attribute_def) when is_list(keys) and is_atom(name) and is_list(attribute_def) do
    v = Keyword.values(attribute_def)
    {:atomic, s} = t(fn() ->
      qc = Qlc.q("""
            [Type || {_, N, _, _} <- Q, Type <- Types,
             N =:= Type]
             """, [Q: Reltype.table(), Types: v])
      Qlc.e(qc)
    end)
    IO.inspect [s: s, v: v]
    case (v -- s) do
      [] ->
        attribute_keys = Keyword.keys(attribute_def)
        attributes = [:key|attribute_keys]
        tabledefs = [type: :set,
                     attributes: attributes,
                     user_properties: [keynames: keys,
                                       types: attribute_def]]
#        IO.inspect [create_var: name, tabledefs: tabledefs]
        {:atomic, :ok} = :mnesia.create_table(name, tabledefs)
        %__MODULE__{name: name, 
                    types: attribute_def, 
                    keys: keys, 
                    attributes: attributes}
      r ->
        :mnesia.abort({:attribute_type_unmatch, 
                       name, 
                       %{types: attribute_def, 
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
  @spec valid(tuple, [{atom, any}]) :: list
  def valid(v, types) when is_list(v) and is_list(types) do
#    IO.inspect [v: v, types: types]
    qc = Qlc.q("""
          [{Name, {TypeName, V}} || 
             {Name, Value} <- AttributeSet,
             {AName, TName} <- Type,
             {_,TypeName, Definition, _} <- RelType,
             TName =:= TypeName,
             Name =:= AName,
             Definition(V = element(2,lists:keyfind(Name, 1, AttributeSet))) =:= false]
    """, [AttributeSet: v,
          Type: types, RelType: Reltype.table])
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
  def get_indexies(attributes, items) do
#    IO.inspect [get_indexies: {attributes, items}]
    Enum.reduce(items, [], fn(x, a) ->
      [Enum.find_index(attributes, &(x === &1))|a]
    end)
    |> Enum.reverse()
  end
  def get_keys(t, index_list) do
    IO.inspect([:t, t])
    r = Enum.map(index_list, &(elem(t, &1))) 
        |> List.to_tuple()
    case r do
      {i} -> i
      _ -> r
    end
  end
  def get_key_from_tuple(t, relvar) do
    [_key|attributes] = relvar.attributes
    keys = relvar.keys
    i = get_indexies(attributes, keys)
    get_keys(t, i)
  end
  @doc """
  write relational value into relational variable(relvar) as name

  input :  {%{key1: aaa}, %{att1: value1, attr2: value2}}
  translate: {table_name, %{key1: aaa}, value1, value2}
  """
  @type reason :: tuple
  @spec write(atom|__MODULE__.t, tuple) :: :ok | {reason, atom, map}
  def write(relvar = %__MODULE__{}, t) do
    types = relvar.types
    name = relvar.name
    keyitem = get_key_from_tuple(t, relvar)
    attributes = relvar.attributes
    z = Enum.zip(attributes, [keyitem|Tuple.to_list(t)])
    
    case valid(z, types) do
      [] ->
        Tuple.insert_at(t, 0, keyitem)
        |> Tuple.insert_at(0, name)
        |> :mnesia.write()
##        |> Constraint.validate
      ret ->
        :mnesia.abort({:typecheck_error, name, Enum.into(ret, %{})})
    end
  end
  def write(name, t) when is_atom(name) and is_tuple(t) do
    n = to_relvar(name)
    write(n, t)
  end
  def write(relvar = %__MODULE__{}, t) when is_map(t) do
    write(relvar, map_to_tuple(t, relvar))
  end
#  def insert(left, relvar = %__MODULE__) when is_map(left) do
#    Enum.map(left, &(write(relvar, &1)))
  #  end
  def map_to_tuple(new, relvar) do
    keys = relvar.keys
    [:key|attributes] = relvar.attributes
    :erlang.list_to_tuple(Enum.map(attributes, fn(e) -> Map.get(new, e) end))
  end
  def update_or_replace(new, old, relvar) do
    keys = relvar.keys
    new = Map.merge(old, new);
    {new_keys, _} = Map.split(new, keys)
    {old_keys, _} = Map.split(old, keys)
#    new_keys = List.to_tuple(Enum.map(keys, &(new_keys[&1])))
#    old_keys = List.to_tuple(Enum.map(keys, &(old_keys[&1])))
    if (!Map.equal?(new_keys, old_keys)) do
      t = map_to_tuple(old_keys, relvar)
      delete(relvar, t)
    end
    t = map_to_tuple(new, relvar)
    IO.inspect([update_or_replace: t])
    write(relvar, t)
  end
  def constraint(new, old, relvar) do
    case Enum.all?(relvar.constraints, &(&1.(new, old, relvar))) do
      true -> new
      false -> raise(RuntimeError, "constraint violation");
    end
  end
  @spec update(relval, __MODULE__.t) :: relval
  def update(left, %__MODULE__{} = relvar) when is_map(left) do
    Enum.map(left, fn(t) -> 
      write(relvar, t) 
    end)
  end
  @spec update(relval, (tuple -> tuple), __MODULE__.t) :: relval
  def update(relval, updatefn, %__MODULE__{} = relvar) do
    IO.inspect [update: relval]
    Enum.map(relval.body, fn(x) -> 
      IO.inspect [relval: x]
      old_map = L.to_map(x, relval.types)
      IO.inspect [oldmap: old_map]
      updatefn.(old_map) |>
#      constraint(old_map, relvar)|>
#      merge_map_to_tuple(old, relvar) |>
      update_or_replace(old_map, relvar)
    end)
  end
  def delete(left, %__MODULE__{} = relvar) when is_map(left) do
    Enum.map(left, fn(t) -> delete(relvar, t) end)
  end
  @spec delete(__MODULE__.t, keys) :: :ok | :no_return
  def delete(relvar, t) do
    keyitem = get_key_from_tuple(t, relvar)
    :mnesia.delete({relvar.name, keyitem})
  end
  
end
defimpl Enumerable, for: Relvar2 do
  alias Relvar2, as: R
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
    keyitem = R.get_key_from_tuple(val, relvar)
#    IO.inspect [enum_member: val, key: keyitem]
    r = case :mnesia.read(relvar.name, keyitem) do
          [] -> 
            false
          [x] -> 
            t = R.record_to_tuple(x)
            case t do
              ^val -> true
              _ -> false
            end
        end
#    IO.inspect [enum_member: val, key: keyitem, r: r]
    {:ok, r}
#    [table(relvar), keyitem])
#    {:error, __MODULE__}
  end
  def reduce(_, {:halt, acc}, _fun), do: {:halted, acc}
  def reduce(v, {:suspend, acc}, fun) do
    {:suspended, acc, &(reduce(v, &1, fun))}
  end
  def reduce(v, {:cont, acc}, fun) do
#    Logger.debug fn() -> inspect([reduce: v]) end
#    [:key|attribute_set] = v.attributes
    {_, ret} = :mnesia.foldl(
      fn(e, {:cont, acc}) ->
          t = R.record_to_tuple(e)
#         IO.inspect [v: v, acc: acc, t: t]
          fun.(t, acc)
        (_e, {r, acc}) ->
          {r, acc}
      end, 
      {:cont, acc}, v.name)
#    IO.inspect [fold_ret: ret]
    {:done, ret}
  end
end
defimpl Collectable, for: Relvar2 do
  alias Relvar2, as: R
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
