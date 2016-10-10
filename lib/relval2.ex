defmodule Relval2 do
  alias Relvar2, as: R
  alias Reltuple, as: T
  require Qlc
  require Logger
  defstruct types: Keyword.new(), body: MapSet.new()
  @type key_name :: atom
  @type key_val :: any
  @type key_set :: %{key_name => key_val}
  @type value_set :: %{value_name => value_val}
  @type value_name :: atom
  @type value_val :: any
  @type relval :: %{key_set => value_set}
  def iterator(relval) do
    IO.inspect [relval: relval]
    case Enum.split(relval.body, 1) do
      {[], []} -> []
      {[h], t} -> fn() -> qlc_next([h|t]) end
    end
  end
  def qlc_next([h|t]) do
    [h|fn() -> qlc_next(t) end]
  end
  def qlc_next([]) do
    []
  end
  def qlc_next(x) when is_function(x) do
    x.()
  end
  def new(types: type, body: body) do
    %__MODULE__{types: type, body: body}
  end
  @doc """
  qlc table function.

  
  """
  def table(relval) do
    tf = fn() -> qlc_next(iterator(relval)) end
    infofun = fn
      (:num_of_objects) -> MapSet.size(relval.body)
      (:keypos) -> 1
      (:is_sorted_key) -> false
      (:is_unique_objects) -> true
      (_) -> :undefined
    end
    formatfun = fn({:all, nelements, elementfun}) ->
      IO.inspect [all: :all, nelements: nelements, elementfun: elementfun]
      list = MapSet.to_list(relval.body)
      {e, p} = if (MapSet.size(relval.body) > nelements) do
        {Enum.drop(list, nelements), :"..."}
      else
        {list, []}
      end
      h = Enum.map(e, fn(x) -> elementfun.(x) end) ++ p
      v = :io_lib.format('~w:new([{body, ~w:new(~w)}])', 
                         [__MODULE__, MapSet, h])
      :io_lib.format('~w:table(~s)', [__MODULE__, v])
    end
    lookupfun = :undefined
    :qlc.table(tf, [info_fun: infofun, format_fun: formatfun,
                    lookup_fun: lookupfun
    ])
  end

  @spec union(R.t | relval, R.t | relval) :: relval
  @doc """
  union left and right.

  left and right should have same keys and attributes.
  """
  def union(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.union(left.body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def union(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.union(left_body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  @spec minus(R.t | relval, R.t | relval) :: relval
  def minus(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.difference(left.body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def minus(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.difference(left_body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  @spec intersect(R.t | relval, R.t | relval) :: relval
  def intersect(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.intersection(left.body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def intersect(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.intersection(left_body, right.body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def intersect(left = %__MODULE__{}, right = %R{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        right_body = MapSet.new(right)
        body = MapSet.intersection(left.body, right_body)
        %__MODULE__{right | body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def to_map(relval_element, types) do
    IO.inspect [tuple_to_map: relval_element, att: types]
    rest = Tuple.to_list(relval_element)
    arest = Keyword.keys(types)
    v = Enum.zip(arest, rest)|> Map.new
    v
  end
  def r_to_t(r, types) do
    Tuple.delete_at(r, 0)
    |> Tuple.delete_at(0)
    |> T.new(types)
  end
  def record_to_map(r, types) do
    IO.inspect [record_to_map: r, att: types]
    [_, _k|rest] = Tuple.to_list(r)
    arest = Keyword.keys(types)
#    IO.inspect [tuple_to_map: arest]
    v = Enum.zip(arest, rest)|> Map.new
    v
  end

  @spec where(R.t | relval, (T.t -> boolean)) :: relval
  def where(left = %R{}, f) do 
#    IO.inspect [left: left.attributes]
    handle = R.table(left)
    att = left.types
    qlc = Qlc.q("""
    [ erlang:delete_element(1, erlang:delete_element(1, X)) || X <- Rel, 
          F(R=TM(X, A)) =:= true]
    """, [Rel: handle,
          A: att,
          F: f,
          TM: fn(x,a) -> r_to_t(x, a) end])
    r = Qlc.e(qlc) |>
      MapSet.new()
    %__MODULE__{types: left.types, 
                body: r}
  end
  def where(left = %__MODULE__{}, f) do
    r = Enum.filter(left.body, fn(x) ->
      f.(T.new(x, left.types)) 
    end) 
    IO.inspect [where_filtered: r, types: left.types]
    %__MODULE__{types: left.types, 
                body: MapSet.new(r)}
  end
  def field_index(types, attribute) do
    Enum.find_index(types, fn({n, _e}) -> attribute == n end)
  end
  def select_fields(attributes, types) do
    ret = Stream.with_index(types) |>
      Stream.filter(fn({{n, _e}, _i}) -> 
        r = n in attributes 
        #      IO.inspect [n: n, e: e, i: i, a: attributes, r: r]
        r
      end) |>
      Enum.unzip()
#    IO.inspect [select_fields: ret]
    ret
  end
  def select_fields_index(attributes, types) do
    Enum.reduce(attributes, [],
             fn(k, a) ->
               case field_index(types, k) do
                 nil -> a
                 r -> [r | a]
               end
             end) |> Enum.reverse()
  end
  @spec project(R.t | relval, [value_name|key_name], boolean) :: relval
  def project(left = %__MODULE__{}, attributes, take \\ true) do
    #r = select_fields_index(attributes, left.types)
    attributes = case take do
                   true -> attributes
                   false -> fields(left) -- attributes
                 end
    {types, indexies} = select_fields(attributes, left.types)
    ret = Stream.map(left.body, fn(v) ->
#      IO.inspect [v: v, r: indexies, take: take, types: types]
      take_set = Enum.reduce(indexies, {}, fn(x, a) ->
        Tuple.append(a, elem(v, x))
      end)
      take_set
    end) |> MapSet.new()
    %__MODULE__{body: ret,
                types: types}
  end
  def project(left = %R{}, attributes, take) do
    attributes = case take do
                   false ->
                     fields(left) -- attributes
                   true ->
                     attributes
                 end
    r = select_fields_index(attributes, left.types)
    types = Enum.map(attributes, &({&1, Keyword.get(left.types, &1)}))
    IO.inspect [project: [r: r, attributes: attributes, types: left.types]]
    body = Enum.map(left, 
                    fn(v) ->
 #                     IO.inspect [project_elem: v, r: r]
                      Enum.reduce(r, {}, fn(x, a) ->
                        Tuple.append(a, elem(v, x))
                      end)
                    end)
    IO.inspect [project: body, types: types]
    %__MODULE__{body: MapSet.new(body), types: types}
  end

  @spec fields(R.t|relval) :: list
  def fields(left = %R{}) do
    [:key|att] = left.attributes
    att
  end
  def fields(left = %__MODULE__{}) do
    att = Keyword.keys(left.types)
    att
  end
  @doc """
  
  """
  def rename(left = %__MODULE__{}, old, newname) do
    i = Enum.map(left.types,
                 fn({k, v}) ->
                   case k === old do
                     true -> {newname, v}
                     false -> {k, v}
                   end
      end)
    %__MODULE__{left| types: i}
  end
  def rename(left = %R{}, old, newname) do
    i = Enum.map(left.types,
                 fn({k, v}) ->
                   case k === old do
                     true -> {newname, v}
                     false -> {k, v}
                   end
      end)
    keys = Enum.map(left.keys, 
                    fn(k) ->
                      case k === old do
                        true -> newname
                        false -> k
                      end
                    end)
    %R{left| types: i, keys: keys}
  end
  @doc """
  add name attribute to relval.
  """
  @spec extend(R.t|relval, atom, any, (value_set -> any)) :: relval
  def extend(relval, name, type, f) do
    body = Enum.map(relval, fn(r) -> 
      Tuple.append(r, f.(r))
    end)|>MapSet.new()
    types = relval.types ++ [{name, type}]
    %__MODULE__{types: types, body: body}
  end
  def destract_keys(left, right) do
    ld = left -- right
    rd = right -- left
#    left -- ld == right -- rd
    {left -- ld, ld, rd}
  end
  @spec join(R.t|relval, R.t|relval, (map, map, map -> map)) :: %__MODULE__{}
  def join(left, right, f) do
    la = fields(left) 
    ra = fields(right)
    {fjkeys, _lrest, _rrest} = destract_keys(la, ra)
#    IO.inspect [fjkeys: fjkeys, ra: ra, lrest: lrest, rrrest: rrest]
#    IO.inspect [fjkeys: fjkeys, left_types: left.types]
    {_ltypes, ls} = select_fields(fjkeys, left.types)
#    IO.inspect [ltypes3: ltypes, ls: ls]
    {rtypes, rs} = select_fields(fjkeys, right.types)
#    IO.inspect [rtypes: rtypes, rs: rs, right_types: right.types]
#    IO.inspect [ltypes: ltypes, ls: ls, left_types: left.types]
    {_ra, r_index} = Enum.with_index(ra) |> Enum.unzip()
    r_rest_index = r_index -- rs
    {_la, l_index} = Enum.with_index(la) |> Enum.unzip()
    l_rest_index = l_index -- ls
    
 #   IO.inspect [r_rest_index: r_rest_index, ra: ra, ls: ls, rs: rs]
    r_rest_types = right.types -- rtypes;
    head = left.types ++ r_rest_types;
#    IO.inspect([left: left, right: right])
    body = for le <- left, re <- right,
      (_k = Enum.map(ls, &(elem(le, &1)))) == Enum.map(rs, &(elem(re, &1))),
      right_rest = Enum.map(r_rest_index, &(elem(re, &1))) do
#        IO.inspect([le: le, re: re])
        s = f.({left.types, le, l_rest_index}, {right.types, re, r_rest_index})
#        IO.inspect([ss: s])
        s
      end
#    IO.inspect([body: body])
    %__MODULE__{types: head, body: MapSet.new(body)}
  end
  @spec fnjoin(R.t | relval, R.t | relval) :: relval
  @doc """
  natural join left and right.

  left and right are RelationalVariable or retational value(relval)
  result map's key part are 
   common attribute, left only attribute, right only attribute.
  value part is %{}.
  """
  def fnjoin(left, right) do
    join(left, right, fn({_ltype, le, _lrest}, {_rtype, re, rrest}) ->
#      IO.inspect [lrest: lrest, rrest: rrest, le: le, re: re]
      s = Enum.reduce(rrest, le, 
                      fn(x, a) ->
                        Tuple.append(a, elem(re, x))
                      end)
#      IO.inspect [s: s]
      s
    end)
  end
  def keys_adjust(fjkeys, keys, %R{} = relvar) do
    if (fjkeys == relvar.keys && length(keys) == 1) do
      [-1]
    else
      keys
    end
  end
  def delete_elements_from_tuple(first, indexs, tuple) do
    s = Enum.drop(Tuple.to_list(tuple), first)
    r = Enum.to_list(0..(length(s)-1))
    Enum.map(r -- indexs, fn(i) -> Enum.at(s, i) end)
  end
  def njoin(left, right) do
    la = fields(left) 
    ra = fields(right)
    {fjkeys, _lrest, _rrest} = destract_keys(la, ra)
    lkeys = select_fields_index(fjkeys, left.types)
    lkeys = keys_adjust(fjkeys, lkeys, left)
    rkeys = select_fields_index(fjkeys, right.types)
    rkeys = keys_adjust(fjkeys, rkeys, right)
    IO.inspect [fjkeys: fjkeys, lkeys: lkeys, rkeys: rkeys, rkey: right.keys]
    s = Enum.zip(lkeys, rkeys) |>
      Enum.map(fn({lkey, rkey}) -> 
        "element(#{lkey+3}, Left) =:= element(#{rkey+3}, Right)"
      end) |>
      Enum.join(",") |> 
      to_char_list()
    s0 = '[ list_to_tuple(F(Left) ++ FK(Right)) || '
    s1 = 'Left <- L, Right <- R, '
    s2 = s0 ++ s1 ++ s ++ ' ].'
    q = :qlc.string_to_handle(s2, [], 
                              [L: R.table(left),
                               R: R.table(right),
                               F: fn(x) -> 
                                  y = delete_elements_from_tuple(2, [], x)
#                                  IO.inspect [y: y, x: x]
                                  y
                                end,
                               FK: fn(x) ->
#                                  IO.inspect [rkeys: rkeys]
                                  y = delete_elements_from_tuple(2, rkeys, x)
#                                  IO.inspect [y2: y, x: x]
                                  y
                                end
                              ])
    IO.puts :qlc.info(q)
    :qlc.e(q)
  end

  @spec matching(R.t | relval, R.t | relval) :: relval
  @doc """
  semijoin left, right 
  """
  def matching(left, right) do
    join(left, right, fn(left_comp, left_rest, _right_rest) ->
      {Map.merge(left_comp, left_rest), %{}}
    end)
  end
  def gourping(e_var = %R{}, map_val, fun) do
    Enum.reduce(e_var, map_val, fn(entry, categories) ->
      Map.update(categories, fun.(entry), [entry], 
                 fn %{} ->
                     [entry]
                   x ->
                     [entry|x]
                 end)
    end)
  end
  def grouping(e_val, map_var = %R{}, fun) do
    Enum.reduce(e_val.body, map_var, fn(entry, categories) ->
      Map.update(categories, fun.(entry), [entry], 
                 fn %{} ->
                     [entry]
                   x ->
                     [entry|x]
                 end)
    end)
  end
  def grouping(e_val, map_val, fun) do
    Enum.reduce(e_val, map_val.body, fn(entry, categories) ->
      IO.inspect [e_val: e_val, map_val: map_val.body, entry: entry, categories: categories, 
                 fun_entry: fun]
      
      Map.update(categories, fun.(entry), [entry], 
                 fn %{} ->
                     [entry]
                   x ->
                     [entry|x]
                 end)
    end)
  end
  @spec count(tuple,  %__MODULE__{}) :: tuple
  def count(t, v) do
    IO.inspect [count_v: v, t: t]
    s = MapSet.size(v.body)
    IO.inspect [count_v2: t, s: s]
    %T{t | :tuple => Tuple.append(t.tuple, s)}
  end
  @spec max(map, %__MODULE__{}, atom) :: map
  def max(t, v, target_label) do
    IO.inspect [max_v: v, t: t]
    k = Enum.with_index(Keyword.keys(v.types))
    r = case MapSet.size(v.body) do
          0 -> 0
          _ ->
            {min, max} = Enum.min_max_by(v.body, fn(x) -> 
              IO.inspect [x: x, k: k, target_label: target_label]
              elem(x, k[target_label])
            end)
            elem(max, k[target_label])
        end
    %T{t | :tuple => Tuple.append(t.tuple, r)}
  end
  @doc """
  summarize operator

  summarize left per right, calculate and add new tuple value.
  """
  @spec summarize_old(relval|Relvar.t, relval|Relvar.t, [add: (map, {key_set, value_set} -> map)]) :: relval
  def summarize_old(left, right, [add: summary_list_fun]) do
    g = grouping(left, right, 
                 fn({k, _v}) -> 
                   {km, _vm} = Enum.at(right, 0)
                   keys = Map.keys(km)
                   sk = Map.take(k, keys)
                   case Map.get(right, sk) do
                     nil ->
                       nil
                     _v ->
                       sk
                   end
                 end)
#    IO.inspect [grouped: g]
    Enum.filter_map(g, fn({k, _v}) -> 
      case k do
        nil -> false
        _ -> true
      end
    end,
      fn({k,v}) -> 
        {k, case v do
              %{} ->
                summary_list_fun.(%{}, %{})
              v ->  
                Enum.reduce(v, %{}, 
                            fn ({ks, vs}, a) -> 
                              summary_list_fun.(a, Map.merge(ks, vs))
                            end)
            end}
      end)
  end
  @doc_2 """
  iex> summarize(a, a(:ap)) add: do
  maxq <- max(qty)
  minq <- min(qty)
  end
  """
  def summarize(left, right, [add: {summary_fun, summary_types}]) do
#    IO.inspect [left: project(left, [:sno, :pno, :qty])]
    IO.inspect [left: left]
    r = Enum.map(right, fn(x) ->
      IO.inspect [x: x]
      right_attrs = Keyword.keys(right.types)
      p = Reltuple.new(x, right.types)
      pivot = to_map(x, right.types)
      pivot_keys = Map.keys(pivot)
      r = Enum.reduce(left, [], fn(e, a2) ->
#        IO.inspect [x: x, e: e, left: project(left, [:sno, :pno, :qty]), a2: a2]
#        IO.inspect [pivot: pivot, pivot_keys: pivot_keys]
        IO.inspect [p: p, right_attrs: right_attrs]
        attributes = Reltuple.new(e, left.types)
        IO.inspect [attributes: attributes]
        etuple = Reltuple.take(attributes, right_attrs)
#        attributes = to_map(e, left.types)
#        emap = Map.take(attributes, pivot_keys)
        IO.inspect [etuple: etuple, p: p]
        if (Reltuple.equal?(p, etuple)) do
          [attributes.tuple | a2]
        else
          a2
        end
      end)
      IO.inspect [x: right, r: r]
      {p, r}
##      |> summary_fun.()
##      [x ++ r|a]
    end)
    s = Enum.map(r, fn({x, e}) -> 
      t = summary_fun.(x, %__MODULE__{body: MapSet.new(e), 
                                  types: left.types})
      t.tuple
    end)
    %__MODULE__{:body => MapSet.new(s), :types =>  right.types ++ summary_types}
  end
  def new(body, types) do
    %__MODULE__{types: types, body: body}
  end
end

defimpl Enumerable, for: Relval2 do
  alias Relvar2, as: R
  require Logger
  @spec count(R.t) :: {:ok, non_neg_integer} | no_return
  def count(_v) do
    {:error, __MODULE__}
  end
  def member?(_relvar, _val) do
    {:error, __MODULE__}
  end
  def reduce(_, {:halt, acc}, _fun), do: {:halted, acc}
  def reduce(v, {:suspend, acc}, fun) do
    {:suspended, acc, &(reduce(v, &1, fun))}
  end
  def reduce(v, {:cont, acc}, fun) do
    Enumerable.MapSet.reduce(v.body, {:cont, acc}, fun)
  end
end
defimpl Collectable, for: Relval2 do
  alias Relval2, as: R
  def into(v) do
    {v, fn 
      (relvar, {:cont, x}) -> 
#        IO.inspect [into: x, body: relvar.body]
        body = MapSet.put(relvar.body, x)
#        IO.inspect [into2: body]
        %R{relvar | body: body}
      (relvar, :done) ->
        relvar
      (_relvar, :halt) -> 
        :ok
    end}
  end
end
