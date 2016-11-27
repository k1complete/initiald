defmodule Relval2 do
  alias Relvar2, as: R
  alias Reltuple, as: T
  require Qlc
  require Logger
  defstruct types: Keyword.new(), keys: [], body: MapSet.new(), query: nil
  @rel  :_relname
  @key  :_key
  @type key_name :: atom
  @type key_val :: any
  @type key_set :: %{key_name => key_val}
  @type value_set :: %{value_name => value_val}
  @type value_name :: atom
  @type value_val :: any
  @type t :: %__MODULE__{types: Keyword.t,
                         keys: list(),
                         body: MapSet.t,
                         query: nil | :qlc.qlc_handle()
  }
  """
  @spec iterator(%__MODULE__{}) :: []
  def iterator(relval) do
#    IO.inspect [iterator_relval: relval]
    case Enum.split(relval.body, 1) do
      {[], []} -> []
      {[h], t} -> fn() -> qlc_next([h|t]) end
    end
  end
  def qlc_next([h|t]) do
#    IO.inspect [qlc_next: h, t: t]
    [h|fn() -> qlc_next(t) end]
  end
  def qlc_next([]) do
    []
  end
  def qlc_next(x) when is_function(x) do
    x.()
  end
  """
  @doc """
  qlc table function.

  """
  """
  def table(relval) do
    tf = fn() -> qlc_next(iterator(relval)) end
    infofun = fn
      (:num_of_objects) -> MapSet.size(relval.body)
      (:keypos) -> 1
      (:is_sorted_key) -> false
      (:is_unique_objects) -> true
      (_x) -> #IO.inspect [info_fun: _x]
        :undefined
    end
    formatfun = fn({:all, nelements, elementfun}) ->
#      IO.inspect [all: :all, nelements: nelements, elementfun: elementfun]
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
  """
  def table(relval) do
    relval.query
  end
  def new(%{types: type, body: body, keys: keys, name: n} = p) do
    ki = Keyword.keys(type) |> Enum.with_index()
    query = Enum.map(body, fn(x) ->
      y = Tuple.to_list(x)
      ky = Enum.map(keys, &(Enum.at(y, ki[&1])))
      List.to_tuple([n, List.to_tuple(ky) | y])
    end)
    %__MODULE__{types: type, query: query, keys: keys}
  end
  def execute(relval) do
    IO.puts :qlc.info(relval.query)
    Qlc.e(relval.query)
  end
  def sort_tuple(tuple, orders, indexs, keys) do
    IO.inspect [sort_tuple: {tuple, orders, indexs}]
    r = Enum.map(orders, fn(y) -> 
      t = elem(tuple,indexs[y]) 
      if (y == @key) do
        Enum.map(keys, fn(z) -> elem(tuple, indexs[z]) end) 
        |> List.to_tuple()
      else
        t
      end
    end)
    |> List.to_tuple()
    IO.inspect [sort_tuple: {tuple, orders, indexs}, r: r]
    r
  end
  def set_operation(f, left, right) do
    case left.types == right.types && left.keys == right.keys do
      true ->
        types = [@relname, @key | Keyword.keys(left.types)]
        rtypes = Enum.with_index([@relname, @key | Keyword.keys(right.types)])
        q = f.(left.query, right.query)
        IO.puts :qlc.info(q)
        %__MODULE__{types: right.types, query: q}
      false ->
        {:error, :bad_reltype}
    end
  end
  def union2(left, right) do
    fn(lq, rq) ->
      Qlc.q("[X || X <- Q]",
            [Q: :qlc.append(lq, rq)],
            [unique: true])
    end
    |> set_operation(left, right)
  end
  def intersect2(left, right) do
    fn(lq, rq) ->
      Qlc.q("[X || X <- Q1, Y <- Q2, X =:= Y]", 
            [Q1: lq, Q2: rq],
            [unique: true])
    end
    |> set_operation(left, right)
  end
  def is_exists(query, e) do
    r = Qlc.q("[true || X <- Q, X =:= E]", [Q: query, E: e], 
              [unique: true])
    r
  end
  def filter(query, f) do
    Qlc.q("[X || X <- Q, F(X)]", [Q: query, F: f],
          [unique: true])
  end
  def minus2(left, right) do
    fn(lq, rq) -> 
      r = &( (is_exists(rq, &1 ) |> Qlc.e()) == [] )
      filter(lq, r)
    end
    |> set_operation(left, right)
  end
  def extract_common_keys(la, ra) do
    {fkeys, rarest} = Enum.split_with(ra, fn(x) -> Enum.member?(la, x) end)
  end
  def njoin(left, right, pf, pt, pk) do
    la = Keyword.keys(left.types)
    ra = Keyword.keys(right.types)
    {fkeys, rarest} = extract_common_keys(la, ra)
    lai = Enum.with_index(la)
    rai = Enum.with_index(ra)
    s = Enum.map(fkeys, fn(x) -> 
      "element(#{lai[x]+3}, Left) =:= element(#{rai[x]+3}, Right)"
    end) |> Enum.join(",") |> to_char_list()
    IO.inspect [njoin: s, fkeys: fkeys, lai: lai, rai: rai]
    IO.inspect [left: Qlc.e(left.query), right: Qlc.e(right.query)]
    s0 = '[ F(Left, Right) || '
#    s0 = '[ { Left, Right } || '
    s1 = 'Left <- L, Right <- R, '
    s2 = s0 ++ s1 ++ s ++ ' ].'
    IO.inspect [string: s2, right_keys: right.keys, rarest: rarest]
    types = pt.(left.types, rarest, right.types)
    keys = pk.(left.keys, fkeys, right.keys)
    q = :qlc.string_to_handle(s2, [], 
                              [L: left.query,
                               R: right.query,
                               F: fn(x, y) -> pf.(x, fkeys, y, rai, rarest) end
                              ])
    IO.puts :qlc.info(q)
    IO.inspect [Q: :qlc.eval(q)]
    IO.inspect [ret: %{types: types, keys: keys, query: q, lkey: left.keys,
                       rkey: right.keys,
                       ltype: left.types,
               rtype: right.types,
               fkeys: fkeys}]
    %__MODULE__{types: types, keys: keys, query: q}
#    IO.inspect [types: Keyword.drop(right.types, fjkeys)]
  end
  def matching2(left, right) do
    semijoin(left, right)
  end
  def semijoin(left, right) do
    njoin(left, right, 
          fn(x, _fk, _y, _rai, _rarest) -> x end, 
          fn(l, _c, _r) -> l end, 
          fn(l, _fk, _r) -> l end)
  end
  def join2(left, right) do
    f = fn(x,y) -> :"#{x}_#{y}" end
    njoin(left, right, 
          fn(x, fkeys, y, rai, rarest) -> 
            tl = elem(x, 0)
            tr = elem(y, 0)
            t = f.(tl,tr)
            kl = Tuple.to_list(elem(x, 1))
            kr = Tuple.to_list(elem(y, 1))
            k = List.to_tuple(kl ++ (kr -- 
              Enum.map(fkeys, fn(x) -> 
                elem(y, rai[x]+2) 
              end)))
            rrest = Enum.map(rarest, fn(x) ->
              elem(y, rai[x]+2)
            end)
            xrest = Tuple.to_list(x) |> Enum.drop(2)
            y = [t, k | xrest ++ rrest]
#            IO.inspect [y: y, x: x]
            List.to_tuple(y)
          end, 
          fn(l, rarest, r) -> 
            l ++ Enum.map(rarest, &({&1, r[&1]}))
          end, 
          fn(l, fkeys, r) -> 
            l ++ (r -- fkeys)
          end)
  end
  @spec union(R.t | %__MODULE__{}, R.t | %__MODULE__{}) :: %__MODULE__{} | {:error, :bad_header}
  @doc """
  union left and right.

  left and right should have same keys and attributes.
  """
  def union(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.union(left.body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def union(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.union(left_body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  @spec minus(R.t | t, R.t | t) :: t
  def minus(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.difference(left.body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def minus(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.difference(left_body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  @spec intersect(R.t | t, R.t | t) :: t
  def intersect(left = %__MODULE__{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        body = MapSet.intersection(left.body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def intersect(left = %R{}, right = %__MODULE__{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        left_body = MapSet.new(left)
        body = MapSet.intersection(left_body, right.body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def intersect(left = %__MODULE__{}, right = %R{}) do
    case Keyword.equal?(left.types, right.types) do
      true ->
        right_body = MapSet.new(right)
        body = MapSet.intersection(left.body, right_body)
        %__MODULE__{types: right.types, body: body}
      false ->
        {:error, :bad_header}
    end
  end
  def r_to_t(r, types) do
    IO.inspect [r_to_t: r, types: types]
    Relutil.record_to_tuple(r)
    |> T.new(types)
  end
  @doc """
   A and B and C and ...
   keys = [:a, :b, :c]
   exp = quote do: ({a, b, c} == {1, 2, 3} and d == 4)
   -> [key == {1, 2, 3} and d == 4]
  """
  def trans(exp, keys) do
    Macro.prewalk(exp, [], 
                  fn ({:"==", m, [{:"{}", m2, arg}, s]} = z, acc) ->
                    IO.inspect [trans: z, arg: arg, keys: keys]
                    if (arg == keys) do
                      {{:"==", m, [@key, s]}, acc}
                    else
                      {z, acc}
                    end
                    ({:"==", m, [{{a1, _, nil}, {a2, _, nil}}, s]} = z, acc) ->
                      IO.inspect [trans2: z, arg: {a1, a2}, keys: keys]
                      if ([a1, a2] == keys) do
                        IO.inspect [trans23: z, arg: {a1, a2}, keys: keys]
                        {{:"==", m, [{@key, m, nil}, s]}, acc}
                      else
                        {z, acc}
                      end
                    (z, acc) -> 
                      IO.inspect [trans3: z,  keys: keys]
                      {z, acc}
                  end)
  end
  @doc """
  r.attribute = [:f, @key, :a, :b]
  ex: r |> where2 (a == 4 and b == 5) end
  --> F(M) <- X,
      element(3, M) == 4,
      element(4, M) == 5
  F = case r 
      %R{} -> 
        f = fn(x) -> :erlang.delete_element(1, :erlang.delete_element(1, x)) end
      %L{} -> fn(x) -> x end
      end
     
  -> 
  )
  """
  def do_where(left, exp, binding) do
    IO.inspect [do_where: exp, binding: binding]
    {ckey, key, offset, table} = case left do
                                   %R{} -> 
                                     {left.keys, 
                                      Enum.with_index(Keyword.keys(left.types)), 
                                      3,
                                      R.table(left)}
                                   %__MODULE__{} -> 
                                     {left.keys,
                                      Enum.with_index(Keyword.keys(left.types)), 
                                      3,
                                      __MODULE__.table(left)}
                                 end
    {exp2, acc} = trans(exp, ckey)
    IO.puts Macro.to_string(exp2)
    r = Macro.prewalk(exp2, fn(x) ->
      s = case x do
            {v, m, nil} when is_atom(v) -> 
              if ([v] == ckey or v == @key) do
                {:element, m, [2, {:X, m, nil}]}
              else
                case Keyword.fetch(key, v) do
                  {:ok, i} ->
                    IO.inspect [key: key, v: v, exp2: exp2, offset: offset]
                    s = {:element, m, [i + offset, {:X, m, nil}]}
                  :error ->
                    case Keyword.fetch(binding, v) do
                      {:ok, m} ->
                        m
                      :error ->
                        x
                    end
                end
              end
            _ ->
              x
          end
      #      IO.inspect [s: s]
      s
    end)
    IO.puts n = Macro.to_string(r, &fmt/2)
    q = :qlc.string_to_handle('''
    [ X || X <- Q,
    #{n} ].
    ''', [], :erl_eval.add_binding(:Q, table, :erl_eval.new_bindings()) )
    IO.puts :qlc.info(q)
#    Qlc.e(q)
#    %__MODULE__{types: Map.get(left, :types), query: q}
    q
  end
  def execute(relval) do
    body = Qlc.e(relval.query)|> Enum.map(&(:erlang.delete_element(1, :erlang.delete_element(1, &1))))
    %__MODULE__{types: Map.get(relval, :types), 
                body: MapSet.new(body)}
  end
  def fmt(ast, x) do
#    IO.inspect [fmt: ast, x: x]
    case ast do
      {:"==", m, [a, b]} ->
        r = Macro.to_string(a, &fmt/2) <> " =:= " <> Macro.to_string(b, &fmt/2)
        r
      {:and, m, [a, b]} ->
        Macro.to_string(a, &fmt/2) <> ", " <> Macro.to_string(b, &fmt/2)
      {:or, m, [a, b]} ->
        Macro.to_string(a, &fmt/2) <> "; " <> Macro.to_string(b, &fmt/2)
      x when is_atom(x) ->
       "#{x}"
      _ ->
        x
    end
  end
  defmacro where2(left, binding \\ [], exp) do
#    m = IO.puts Macro.to_string(exp)
    s = Macro.escape(exp)
    q = quote bind_quoted: [left: left, exp: s, binding: binding] do
#      IO.puts Macro.to_string(exp)
#      IO.puts Macro.to_string(mexp)
      %Relval2{types: left.types, query: Relval2.do_where(left, exp, binding)}
    end
#    IO.puts Macro.to_string(q)
    q
  end
  def do_project(left, exp, bool) do
    IO.inspect [project: left.types]
    keys = Enum.with_index(Keyword.keys(left.types), 3)
    IO.inspect [keys: keys, exp: exp]
    s = Enum.map(exp, fn(x) -> "element(#{keys[x]}, X)" end) 
        |> Enum.join(",") 
        |> to_char_list()
    v = :erl_eval.add_binding(:Q, left.query, :erl_eval.new_bindings())
    q = '[ { element(1, X), {#{s}}, #{s} } || X <- Q ].'
    IO.inspect [q: q, v: v]
    ret = %Relval2{
      types: Enum.map(exp, fn(x) -> {x, Keyword.get(left.types, x) } end),
      query: :qlc.string_to_handle(q, [unique: true], v)
    }
    IO.inspect [ret: ret]
    ret
  end
  defmacro project2(left, exp, bool \\ true) do
    s = Macro.escape(exp)
    ret = Macro.prewalk(s, fn({:{}, _, [a, _, nil]}) when is_atom(a) ->
      a
      (x) -> x
    end)
    r = case ret do
          {:{}, _, [:{}, _, x]} -> x
          {a, b} -> [a, b]
        end
    IO.inspect [s: s, exp: exp, r: r]
    quote bind_quoted: [left: left, bool: bool, r: r] do
      Relval2.do_project(left, r, bool)
    end
  end
  @spec where(R.t | t, (T.t -> boolean)) :: t
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
    IO.puts :qlc.info(qlc)
    r = Qlc.e(qlc) |>
      MapSet.new()
    IO.inspect [where_ret: r]
    %__MODULE__{types: left.types, 
                body: r}
  end
  def where(left = %__MODULE__{}, f) do
    r = Enum.filter(left.body, fn(x) ->
      f.(T.new(x, left.types)) 
    end) 
#    IO.inspect [where_filtered: r, types: left.types]
    %__MODULE__{types: left.types, 
                body: MapSet.new(r)}
  end
  def select_fields(attributes, types) do
    ret = Stream.with_index(types) |>
      Stream.filter(fn({{n, _e}, _i}) -> 
        r = n in attributes 
#        IO.inspect [n: n, e: _e, i: _i, a: attributes, r: r]
        r
      end) |>
      Enum.unzip()
#    IO.inspect [select_fields: ret]
    ret
  end
  defp select_fields_index(attributes, types) do
    s = Map.new(Enum.with_index(Keyword.keys(types)))
    r = Enum.map(attributes, &(s[&1]))
#    IO.inspect [select_fiels_index: attributes, types: types, r: r]
    r  
  end
  @spec project(R.t | t, [value_name|key_name], boolean) :: t
  def project(left, attributes, take \\ true)
  def project(left = %__MODULE__{}, attributes, take) do
    #r = select_fields_index(attributes, left.types)
    att = case take do
            true -> attributes
            false -> fields(left) -- attributes
          end
    {types, indexies} = select_fields(att, left.types)
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
    att = case take do
            false ->
              fields(left) -- attributes
            true ->
              attributes
          end
    r = select_fields_index(att, left.types)
    types = Enum.map(att, &({&1, Keyword.get(left.types, &1)}))
    IO.inspect [project: [field_index: r, attributes: attributes, types: left.types]]
    body = Enum.map(left, 
                    fn(v) ->
 #                     IO.inspect [project_elem: v, r: r]
                      Enum.reduce(r, {}, fn(x, a) ->
                        Tuple.append(a, elem(v, x))
                      end)
                    end)
#    IO.inspect [project: body, types: types]
    %__MODULE__{body: MapSet.new(body), types: types}
  end

  @spec fields(R.t|t) :: list
  def fields(left = %R{}) do
    [@key|att] = left.attributes
    att
  end
  def fields(left = %__MODULE__{}) do
    att = Keyword.keys(left.types)
    att
  end
  @doc """
  
  """
  @spec rename(%__MODULE__{}, atom, atom) :: %__MODULE__{}
  def rename(%__MODULE__{} = left, old, newname) do
    i = Enum.map(left.types,
                 fn({k, v}) ->
                   case k === old do
                     true -> {newname, v}
                     false -> {k, v}
                   end
      end)
    %__MODULE__{body: left.body, types: i}
  end
  @spec rename(R.t, atom, atom) :: R.t
  def rename(%R{} = left, old, newname) do
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
    attributes = Enum.map(left.attributes, 
                    fn(k) ->
                      case k === old do
                        true -> newname
                        false -> k
                      end
                    end)
    %R{types: i, keys: keys, attributes: attributes, 
       constraints: left.constraints, name: left.name}
  end
  @doc """
  add name attribute to relval.
  """
  @spec extend(R.t|t, atom, any, (value_set -> any)) :: t
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
  @spec join(R.t| t, R.t|t, (tuple, tuple -> tuple)) :: t
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
      (_k = Enum.map(ls, &(elem(le, &1)))) == Enum.map(rs, &(elem(re, &1))) do
#      Enum.map(r_rest_index, &(elem(re, &1))) do
        #        IO.inspect([le: le, re: re])
        s = f.({left.types, le, l_rest_index}, {right.types, re, r_rest_index})
        #        IO.inspect([ss: s])
        s
    end
    #    IO.inspect([body: body])
    %__MODULE__{types: head, body: MapSet.new(body)}
  end
  @doc """
  natural join left and right.

  left and right are RelationalVariable or retational value(relval)
  result map's key part are 
   common attribute, left only attribute, right only attribute.
  value part is %{}.
  """
#  @spec fnjoin(R.t | t, R.t | t) :: t
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
  defp keys_adjust(fjkeys, keys, %__MODULE__{} = relval) do
    keys
  end
  defp keys_adjust(fjkeys, keys, %R{} = relvar) do
    if (fjkeys == relvar.keys && length(keys) == 1) do
      [-1]
    else
      keys
    end
  end
  def delete_elements_from_tuple(first, indexs, tuple) do
    s = Enum.drop(Tuple.to_list(tuple), first)
#    IO.inspect [delete_elements_from_tuple: tuple, first: first, indexs: indexs]
    r = Enum.to_list(0..(length(s)-1))
    t = Enum.map(r -- indexs, fn(i) -> Enum.at(s, i) end)
#    IO.inspect [delete_elements_from_tuple_ret: t]
    t
  end
  def get_qlc_table(m) do
    case m do
      %R{} -> R.table(m)
      %__MODULE__{} -> __MODULE__.table(m)
    end
  end
  def elempos_adjust(%R{} = _t) do
    3
  end
  def elempos_adjust(%__MODULE__{} = _t)  do
    1
  end
  @spec njoin(R.t, R.t) :: t
  def njoin(left, right) do
    la = fields(left) 
    ra = fields(right)
    {fjkeys, _lrest, _rrest} = destract_keys(la, ra)
    IO.inspect [fjkeys: fjkeys, lrest: _lrest, rrest: _rrest]
    lkeys = select_fields_index(fjkeys, left.types)
    lkeys = keys_adjust(fjkeys, lkeys, left)
    rkeys = select_fields_index(fjkeys, right.types)
    IO.inspect [select_fields_index: rkeys, right: right]
    rkeys = keys_adjust(fjkeys, rkeys, right)
    IO.inspect [keys_adjust: rkeys]
    ra = elempos_adjust(right)
    la = elempos_adjust(left)
    s = Enum.zip(lkeys, rkeys) |>
      Enum.map(fn({lkey, rkey}) -> 
        "element(#{lkey+la}, Left) =:= element(#{rkey+ra}, Right)"
      end) |>
      Enum.join(",") |> 
      to_char_list()
#    IO.inspect [njoin: s]

    s0 = '[ list_to_tuple(F(Left) ++ FK(Right)) || '
    s1 = 'Left <- L, Right <- R, '
    s2 = s0 ++ s1 ++ s ++ ' ].'
    q = :qlc.string_to_handle(s2, [], 
                              [L: get_qlc_table(left),
                               R: get_qlc_table(right),
                               F: fn(x) -> 
                                  y = delete_elements_from_tuple(la - 1, [], x)
#                                  IO.inspect [y: y, x: x]
                                  y
                                end,
                               FK: fn(x) ->
#                                  IO.inspect [ra: ra, rkeys: rkeys]
                                  y = delete_elements_from_tuple(ra - 1, rkeys, x)
#                                  IO.inspect [y2: y, x: x]
                                  y
                                end
                              ])
    IO.puts :qlc.info(q)
#    IO.inspect [left: left.types, right: right.types, fjkeys: fjkeys]
#    IO.inspect [types: Keyword.drop(right.types, fjkeys)]

    %__MODULE__{body: MapSet.new(:qlc.e(q)), 
               types: left.types ++ Keyword.drop(right.types, fjkeys)}
  end

#  @spec matching(R.t | relval, R.t | relval) :: relval
  @doc """
  semijoin left, right 
  """
  def matching(left, right) do
    join(left, right, fn(left_comp, left_rest) ->
      List.to_tuple(Tuple.to_list(left_comp) ++ Tuple.to_list(left_rest))
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
#      IO.inspect [e_val: e_val, map_val: map_val.body, entry: entry, categories: categories, fun_entry: fun]
      
      Map.update(categories, fun.(entry), [entry], 
                 fn %{} ->
                     [entry]
                   x ->
                     [entry|x]
                 end)
    end)
  end
  @spec count(T.t,  %__MODULE__{}) :: T.t
  def count(t, v) do
#    IO.inspect [count_v: v, t: t]
    s = MapSet.size(v.body)
#    IO.inspect [count_v2: t, s: s]
    %T{t | :tuple => Tuple.append(t.tuple, s)}
  end
  @spec max(T.t, %__MODULE__{}, atom) :: T.t
  def max(t, v, target_label) do
#    IO.inspect [max_v: v, t: t]
    k = Enum.with_index(Keyword.keys(v.types))
    r = case MapSet.size(v.body) do
          0 -> 0
          _ ->
            {min, max} = Enum.min_max_by(v.body, fn(x) -> 
#              IO.inspect [x: x, k: k, target_label: target_label]
              elem(x, k[target_label])
            end)
            elem(max, k[target_label])
        end
    %T{t | :tuple => Tuple.append(t.tuple, r)}
  end
  @doc """
  summarize operator

  summarize left per right, calculate and add new tuple value.

  iex> summarize(a, a(:ap)) add: do
  maxq <- max(qty)
  minq <- min(qty)
  end
  """
#  @type summarize(t | R.t, t | R.t) :: t
  def summarize(left, right, [add: {summary_fun, summary_types}]) do
#    IO.inspect [left: project(left, [:sno, :pno, :qty])]
#    IO.inspect [left: left]
    r = Enum.map(right, fn(x) ->
#      IO.inspect [x: x]
      right_attrs = Keyword.keys(right.types)
      p = Reltuple.new(x, right.types)
#      pivot = to_map(x, right.types)
#      pivot_keys = Map.keys(pivot)
      r = Enum.reduce(left, [], fn(e, a2) ->
#        IO.inspect [x: x, e: e, left: project(left, [:sno, :pno, :qty]), a2: a2]
#        IO.inspect [pivot: pivot, pivot_keys: pivot_keys]
#        IO.inspect [p: p, right_attrs: right_attrs]
        attributes = Reltuple.new(e, left.types)
#        IO.inspect [attributes: attributes]
        etuple = Reltuple.take(attributes, right_attrs)
#        attributes = to_map(e, left.types)
#        emap = Map.take(attributes, pivot_keys)
#        IO.inspect [etuple: etuple, p: p]
        if (Reltuple.equal?(p, etuple)) do
          [attributes.tuple | a2]
        else
          a2
        end
      end)
#      IO.inspect [x: right, r: r]
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
  @spec new(MapSet.t, keyword) :: t
  def new(body, types) do
    %__MODULE__{types: types, body: body}
  end
end

defimpl Enumerable, for: Relval2 do
  alias Relvar2, as: R
  alias Relval2, as: L
  require Logger
  @spec count(L.t) :: {:ok, non_neg_integer()}
  def count(%L{} = v) do
    {:ok, MapSet.size(v.body)}
  end
  @spec member?(L.t, any) :: {:ok, boolean}
  def member?(%L{} = v, val) do
    {:ok, MapSet.member?(v.body, val)}
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
