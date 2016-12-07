defmodule Relval do
  require Qlc
  require Logger
  alias Relvar2, as: R
  defstruct types: Keyword.new(), keys: [], query: nil, name: nil
  
  def make_key_from_keys([key]) do
    key
  end
  def make_key_from_keys(keys) do
    List.to_tuple(keys)
  end
  def key_from_keys(key) when is_tuple(key) do
    Tuple.to_list(key)
  end
  def key_from_keys(key) when is_atom(key) do
    [key]
  end
  @type query :: :qlc.qlc_handle()
  @type t :: %__MODULE__{types: Keyword.t,
                         keys: list(),
                         query: nil | query}
  @rel :_relname
  @rel :_key
  def new(%{types: type, body: body, keys: keys, name: name} = p) do 
    ki = Keyword.keys(type) |> Enum.with_index()
    query = Enum.map(body, fn(x) ->
      y = Tuple.to_list(x)
      ky = Enum.map(keys, &(Enum.at(y, ki[&1])))
      List.to_tuple([name, make_key_from_keys(ky) | y])
    end)
    %__MODULE__{types: type, query: query, keys: keys, name: name}
  end
  def raw_new(%{types: type, body: body, keys: keys, name: name} = p) do
    %__MODULE__{types: type, query: body, keys: keys, name: name}
  end
  def table(t) do
    t.query
  end
  @spec execute(t) :: list()
  def execute(%__MODULE__{} = relval) do
    IO.inspect [execute: relval]
    Logger.info(:qlc.info(relval.query))
    Qlc.e(relval.query)
  end
  def execute(e) when is_tuple(e) do
    IO.inspect [execute: e]
    e
  end
  @spec set_operation((query, query, atom -> query), t, t) :: t | {:error, :bad_reltype}
  def set_operation(f, left, right) do
    case left.types == right.types  do
      true ->
        types = [@relname, @key | Keyword.keys(left.types)]
        rtypes = Enum.with_index([@relname, @key | Keyword.keys(right.types)])
        q = f.(left.query, right.query, left.name)
        IO.puts :qlc.info(q)
        %__MODULE__{types: right.types, query: q}
      false ->
        {:error, :bad_reltype}
    end
  end
  @spec union(t, t) :: t
  def union(left, right) do
    fn(lq, rq, name) ->
      Qlc.q("[erlang:setelement(1, X, Y) || X <- Q]",
            [Q: :qlc.append(lq, rq),
             Y: name],
            [unique: true])
    end
    |> set_operation(left, right)
  end
  @spec union(t, t) :: t
  def intersect(left, right) do
    e = left.name
    IO.inspect [intersect: left.name]
    fn(lq, rq, name) ->
      Qlc.q("[X || X <- Q1, Y <- Q2, X =:= setelement(1, Y, M)]", 
            [Q1: lq, 
             M: name,
             Q2: rq],
            [unique: true])
    end
    |> set_operation(left, right)
  end
  @spec is_exists(t, tuple, (tuple -> tuple)) :: t
  def is_exists(query, e, f \\ fn(x) -> x end) do
    r = Qlc.q("[true || X <- Q, F(X) =:= F(E)]", 
              [Q: query, E: e, F: f], 
              [unique: true])
    r
  end
  @spec filter(t, (tuple -> bool) ) :: t
  def filter(query, f) do
    Qlc.q("[X || X <- Q, F(X)]", [Q: query, F: f],
          [unique: true])
  end
  @spec minus(t, t) :: t
  def minus(left, right) do
    fn(lq, rq, name) -> 
      r = &( (is_exists(rq, &1, 
                        fn(x) -> 
                          :erlang.setelement(1, x, name)
                        end) |> Qlc.e()) == [] )
      filter(lq, r)
    end
    |> set_operation(left, right)
  end
  
  def extract_common_keys(la, ra) do
    {fkeys, rarest} = Enum.split_with(ra, fn(x) -> Enum.member?(la, x) end)
  end

  def do_natural_join(left, right, pf, pt, pk) do
    la = Keyword.keys(left.types)
    ra = Keyword.keys(right.types)
    {fkeys, rarest} = extract_common_keys(la, ra)
    lai = Enum.with_index(la, 3)
    rai = Enum.with_index(ra, 3)
    s = Enum.map(fkeys, fn(x) -> 
      "element(#{lai[x]}, Left) =:= element(#{rai[x]}, Right)"
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
  @spec matching(t, t) :: t
  def matching(left, right) do
    do_natural_join(left, right, 
                    fn(x, _fk, _y, _rai, _rarest) -> x end, 
                    fn(l, _c, _r) -> l end, 
                    fn(l, _fk, _r) -> l end)
  end
  @spec join(t, t) :: t
  def join(left, right) do
    f = fn(x,y) -> :"#{x}_#{y}" end
    do_natural_join(left, right, 
          fn(x, fkeys, y, rai, rarest) -> 
            tl = elem(x, 0)
            tr = elem(y, 0)
            t = f.(tl,tr)
#            kl = Tuple.to_list(elem(x, 1))
            kl = key_from_keys(elem(x, 1))
            kr = key_from_keys(elem(y, 1))
            k = List.to_tuple(kl ++ (kr -- 
              Enum.map(fkeys, fn(x) -> 
                elem(y, rai[x]-1) 
              end)))
            k = case k do
                  {m} -> m
                  k -> k
                end
            rrest = Enum.map(rarest, fn(x) ->
              IO.inspect [rrest: rai, x: x, y: y]
                elem(y, rai[x]-1) 
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

  def do_project(left, exp, bool \\ true) do
    IO.inspect [project: left.types]
    key = Keyword.keys(left.types)
    keys = Enum.with_index(key, 3)
    IO.inspect [keys: keys, exp: exp]
    s = Enum.map(exp, fn(x) -> "element(#{keys[x]}, X)" end) 
        |> Enum.join(",") 
        |> to_char_list()
    v = :erl_eval.add_binding(:Q, left.query, :erl_eval.new_bindings())
    k = make_key_from_keys(key)
    IO.inspect [project: k, key: key]
    q = if length(exp) == 1 do
      '[ { element(1, X), #{s}, #{s} } || X <- Q ].'
      else
      '[ { element(1, X), {#{s}}, #{s} } || X <- Q ].'
    end
    IO.inspect [q: q, v: v]
    ret = %Relval{
      name: left.name,
      keys: key,
      types: Enum.map(exp, fn(x) -> {x, Keyword.get(left.types, x) } end),
      query: :qlc.string_to_handle(q, [unique: true], v)
    }
    IO.inspect [ret: ret]
    ret
  end
  def project(left, r, bool \\ true) do
    Relval.do_project(left, r, bool)
  end
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
  def fmt(ast, x) do
#    IO.inspect [fmt: ast, x: x]
    case ast do
      {:"==", m, [a, b]} ->
        r = Macro.to_string(a, &fmt/2) <> " =:= " <> Macro.to_string(b, &fmt/2)
        r
      {:"!=", m, [a, b]} ->
        r = Macro.to_string(a, &fmt/2) <> " =/= " <> Macro.to_string(b, &fmt/2)
        r
      {:and, m, [a, b]} ->
        Macro.to_string(a, &fmt/2) <> ", " <> Macro.to_string(b, &fmt/2)
      {:or, m, [a, b]} ->
        Macro.to_string(a, &fmt/2) <> " orelse " <> Macro.to_string(b, &fmt/2)
      x when is_binary(x) ->
       "<<\"#{x}\">>"
      x when is_atom(x) ->
       "#{x}"
      _ ->
        x
    end
  end
  defmacro where(left, binding \\ [], exp) do
#    m = IO.puts Macro.to_string(exp)
    s = Macro.escape(exp)
    q = quote bind_quoted: [left: left, exp: s, binding: binding] do
#      IO.puts Macro.to_string(exp)
#      IO.puts Macro.to_string(mexp)
      %Relval{types: left.types, query: Relval.do_where(left, exp, binding)}
    end
#    IO.puts Macro.to_string(q)
    q
  end
  def extend(left, f, [{a, t}]) do
    q = Qlc.q("[erlang:append_element(F(R), R) || R <- Left]",
      [F: fn(x) -> f.(x) end,
      Left: left.query])
    %Relval{types: left.types ++ [{a, t}],
            query: q,
            name: left.name,
            keys: left.keys}
  end
  def count(left) do
    r = :qlc.fold(fn(_x, a) -> a + 1 end, 0, left.query)
    IO.inspect [count: r ]
    r
  end
  def min(left, param) do
    :qlc.fold(
      fn(x, nil) -> elem(x, param)
        (x, a) -> 
          y = elem(x, param)
          if (y < a) do
            y
          else
            a 
          end
      end, nil, left.query)
  end
  def max(left, p) do
    param = Keyword.keys(left.types) |> Enum.with_index(2) |> Keyword.get(p)
    :qlc.fold(
      fn(x, nil) -> elem(x, param)
        (x, a) -> 
          y = elem(x, param)
          if (y > a) do
            y
          else
            a 
          end
      end, nil, left.query)
  end
  def summarize(left, right, add: {summary_fun, summary_types}) do
    q = Qlc.q("[list_to_tuple(tuple_to_list(R)++tuple_to_list(F(R))) || R <- Right]",
              [F: 
               fn(x) -> 
                 keys = Keyword.keys(right.types)
                 IO.inspect [XXX: x, name: elem(x, 0), key: keys,
                             types: right.types]
                 summary_fun.(Relval.matching(
                       left, 
                       Relval.raw_new(%{body: [x],
                                        types: right.types,
                                        name: elem(x, 0),
                                        keys: Keyword.keys(right.types)})))
               end,
               Right: right.query])
    IO.inspect [summarize_debug: Relval.execute(q)]
    %Relval{types: right.types ++ summary_types, 
            query: q, 
            name: right.name, keys: right.keys}
  end
end
