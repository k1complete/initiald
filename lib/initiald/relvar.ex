alias InitialD.Constraint
alias InitialD.Relval
alias InitialD.Relvar
alias InitialD.Reltype
alias InitialD.Relutil
alias InitialD.Reltuple

defmodule InitialD.Relvar do
  require Logger
  require Qlc
#  alias Relval2, as: L
  require Reltuple
  alias Reltuple, as: T
  @behaviour Access

  @key :_key
#  @relname :_relname
  defstruct name: nil, types: nil, 
            keys: nil, attributes: nil, constraints: nil,
            query: nil
  @type t :: %__MODULE__{name: atom, 
                         types: Keyword.t, 
                         keys: [atom], 
                         constraints: [Constraint.t]|nil,
                         query: any}
  @type relvar :: %__MODULE__{name: atom, 
                         types: Keyword.t, 
                         attributes: [atom],
                         constraints: [Constraint.t]|nil,
                         query: any}
  def add_index(%__MODULE__{name: v} = _relvar, att) do
    :mnesia.add_table_index(v, att)
  end

  def del_index(%__MODULE__{name: v} = _relvar, att) do
    :mnesia.del_table_index(v, att)
  end
  @attributes :_attributes
  @attribute_fields [name: :atom, type: :atom]
  def init() do
    :mnesia.create_table(@attributes,
      [type: :set,
       attributes: Keyword.keys(@attribute_fields)])
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

  @type keys :: [tuple]
  @type attributes :: [tuple]
  @type relval :: any
#  @type qlc_handle :: :qlc.qlc_handle()
  @type qlc_handle :: any()
  def t(f) do
    :mnesia.transaction(f)
  end
  @spec table(__MODULE__.t | atom) :: qlc_handle
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
  @spec drop(atom | __MODULE__.t) :: :ok | no_return
  def drop(relname) when is_atom(relname) do
#    Logger.debug(fn() -> inspect(relname) <> " drop" end)
    true = :mnesia.activity(:transaction,
      fn() -> 
        q = Qlc.q("""
        [ K || {_, {T, K}, _} <- A,
          T =:= Table ]
        """, [A: :mnesia.table(@attributes),
              Table: relname])
        m = Qlc.e(q)
        Enum.all?(m, fn(e) ->
          :ok = :mnesia.delete({@attributes, e})
        end)
      end)
    :mnesia.delete_table(relname)
  end
  def drop(relvar = %__MODULE__{}) do
    drop(relvar.name)
  end
  @doc """
  create relational variable with keylist and %{attribute_name=>type} set.

  - type checking in attributeset
  """
  @spec create(atom, list, [{atom, any}]) :: %__MODULE__{} | no_return
  def create(name, keys, attribute_def) when is_list(keys) and is_atom(name) and is_list(attribute_def) do
    v = Keyword.values(attribute_def)
    {:atomic, s} = t(fn() ->
      qc = Qlc.q("""
            [Type || {_, N, _, _} <- Q, Type <- Types,
             N =:= Type]
             """, [Q: Reltype.table(), Types: v])
      Qlc.e(qc)
    end)
#    IO.inspect [s: s, v: v]
    case (v -- s) do
      [] ->
        attribute_keys = Keyword.keys(attribute_def)
        attributes = [@key|attribute_keys]
        tabledefs = [type: :set,
                     attributes: attributes]
#                     user_properties: [keynames: keys,
#                                       types: attribute_def]]
#        IO.inspect [create_var: name, tabledefs: tabledefs]
      :ok = :mnesia.activity(:transaction, fn() ->
          Enum.map(attribute_def, fn({n, t}) ->
            :mnesia.write({@attributes, {name, n}, t})
          end)
          :mnesia.write({@attributes, {name, @key}, keys})
        end)
        {:atomic, :ok} = :mnesia.create_table(name, tabledefs)
        %__MODULE__{name: name, 
                    types: attribute_def, 
                    keys: keys, 
                    query: :mnesia.table(name),
                    attributes: attributes}
      r ->
        :mnesia.abort({:attribute_type_unmatch, 
                       name, 
                       %{types: attribute_def, 
                         diff: r}})
    end
  end
  @spec types(atom) :: [{atom, any}]
  defp types(name) when is_atom(name) do
    :mnesia.activity(:transaction, fn() ->
      [@key | attributes] = :mnesia.table_info(name, :attributes)
      Enum.map(attributes, fn(a) -> 
        [{_, {_, a}, v}] =  :mnesia.read(@attributes, {name, a})
        {a, v}
      end)
    end)
  end
  defp keys(name) when is_atom(name) do
    :mnesia.activity(:transaction, fn() ->
      [{_, {^name, @key}, v}] =  :mnesia.read(@attributes, {name, @key})
      v
    end)
  end
  @spec to_relvar(atom) :: %__MODULE__{name: atom, attributes: [atom], }
  def to_relvar(name) when is_atom(name) do
    s = :mnesia.table_info(name, :all)
    %__MODULE__{name: name, 
                attributes: Keyword.get(s, :attributes),
                types: types(name),
                keys: keys(name),
                query: :mnesia.table(name)}
  end
  @doc """
  validate {k, v} record by type map before writing.
  
  output: invalid {attribute, {type, value}} list.
  """
#  @spec valid(list, [{atom, any}]) :: [any()] | {:error,atom(),:bad_object | {:bad_object,atom() | [atom() | [any()] | char()]} | {:bad_term,atom() | [atom() | [any()] | char()]} | {:premature_eof,atom() | [atom() | [any()] | char()]} | {:file_error,atom() | [atom() | [any()] | char()],atom()}}
  @spec valid(list, Keyword.t) :: [any()] | no_return()
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
#    IO.puts :qlc.info(qc)
#    IO.inspect [Type: Qlc.e(Qlc.q("[X || X <- Q]", [Q: Reltype.table]))]
    ret = :qlc.e(qc)
#    IO.inspect [typeret: ret]
    ret
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
#    IO.inspect([t: t, index_list: index_list])
    r = Enum.map(index_list, &(elem(t, &1))) 
        |> List.to_tuple()
        |> Relutil.to_primary_key()
    r
  end
  def get_key_from_tuple(t, relvar) do
    [@key | attributes] = relvar.attributes
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
  @spec write(atom|__MODULE__.t, tuple) :: :ok | none() | no_return()
  def write(relvar = %__MODULE__{}, t) do
    types = relvar.types
    name = relvar.name
    keyitem = get_key_from_tuple(t, relvar)
    attributes = relvar.attributes
    z = Enum.zip(attributes, [keyitem|Tuple.to_list(t)])
#    IO.inspect([z: z, types: types, t: t])
    case valid(z, types) do
      [] ->
        Tuple.insert_at(t, 0, keyitem)
        |> Tuple.insert_at(0, name)
        |> Constraint.verify()
        |> :mnesia.write()
        Constraint.validate([relvar.name])
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
    #keys = relvar.keys
    [@key|attributes] = relvar.attributes
    :erlang.list_to_tuple(Enum.map(attributes, fn(e) -> Map.get(new, e) end))
  end
  @spec update_or_replace(T.t, T.t, __MODULE__.t) :: :ok | none() | no_return()
  def update_or_replace(%T{} = new, %T{} = old, relvar) do
    keys = relvar.keys
#    IO.inspect [update_or_replace: new, old: old, relvar: relvar]
#    new = Map.merge(old, new);
    new_keys = T.take(new, keys)
    old_keys = T.take(old, keys)
    a = if (!T.equal?(new_keys, old_keys)) do
      delete(relvar, old.tuple)
    end
    case a do
      :ok -> 
#    IO.inspect([update_or_replace: t])
        write(relvar, new.tuple)
      _ ->
        write(relvar, new.tuple)
    end
    :ok
  end
  def constraint(new, old, relvar) do
    case Enum.all?(relvar.constraints, &(&1.(new, old, relvar))) do
      true -> new
      _ -> raise(RuntimeError, "constraint violation")
    end
  end
  @spec update(Relval.t,  %__MODULE__{}) :: list | no_return()
  def update(left, %__MODULE__{} = relvar) when is_map(left) do
    Enum.map(left, fn(t) -> 
      write(relvar, t) 
    end)
  end
  def update(relval, updatefn, %__MODULE__{} = relvar) do
#    IO.inspect [update: relval]
    try do
      Enum.map(relval.body, fn(x) -> 
        #      IO.inspect [relval: x]
        old_map = T.new(x, relval.types)
        #      IO.inspect [oldmap: old_map]
        updatefn.(old_map) |>
          #      constraint(old_map, relvar)|>
          #      merge_map_to_tuple(old, relvar) |>
          update_or_replace(old_map, relvar)
      end)
    catch
     {e, r} -> raise(e, r)
    end
  end
  def delete(relvar, t) do
#    IO.inspect [delete_t: t]
    keyitem = get_key_from_tuple(t, relvar)
#    IO.inspect [delete_t_keyitem: keyitem]
    :mnesia.delete({relvar.name, keyitem})
  end
  def get(t, key, default \\ nil) do
    case fetch(t, key) do
      :error -> default
      {:ok, value} -> value
    end
  end
  def get_and_update(t, key, f) do
    Relval.get_and_update(t, key, f)
  end
  def pop(t, key) when is_tuple(key) do
    Relval.pop(t, key)
  end
  def fetch(t, key) when is_tuple(key) do
    Relval.fetch(t, key)
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
    keyitem = R.get_key_from_tuple(val, relvar)
#    IO.inspect [enum_member: val, key: keyitem]
    r = case :mnesia.read(relvar.name, keyitem) do
          [] -> 
            false
          [x] -> 
            t = Relutil.record_to_tuple(x)
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
#    [@key|attribute_set] = v.attributes
    {_, ret} = :mnesia.foldl(
      fn(e, {:cont, acc}) ->
          t = Relutil.record_to_tuple(e)
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
