defmodule InitialD do
  require Relval
  @behaviour Access
  @doc """
   deftype :qpr, [q: list(integer)],
                 [constraint: fn(x) -> %x(:aa)]
  """
  defmacro __using__(_ops) do
    quote do
      require Relval
      require Reltype
      require Relvar2
      require InitialD
      import InitialD
    end
  end
  @doc """
  iex> R.t(fn() ->
  iex>   union(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{12, :a1, :a23},
  ...>                             {14, :a1, :a24},
  ...>                             {16, :a1, :a25}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2}),
  ...>         Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{10, :a1, :a22},
  ...>                             {14, :a1, :a24}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2})) |> 
  ...>   Relval.execute() |>
  ...>   Enum.sort()
  ...> end)
  {:atomic,
   [{:test2, {:a1, :a22}, 10, :a1, :a22},
    {:test2, {:a1, :a23}, 12, :a1, :a23},
    {:test2, {:a1, :a24}, 14, :a1, :a24},
    {:test2, {:a1, :a25}, 16, :a1, :a25}]}

  """
  @spec union(Relval.t, Relval.t) :: Relval.t
  def union(left,right) do
    Relval.union(left, right)
  end

  @doc """
  iex> R.t(fn() ->
  iex>   minus(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{12, :a1, :a23},
  ...>                             {14, :a1, :a24},
  ...>                             {16, :a1, :a25}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2}),
  ...>         Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{10, :a1, :a22},
  ...>                             {14, :a1, :a24}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2})) |> 
  ...>   Relval.execute() |>
  ...>   Enum.sort()
  ...> end)
  {:atomic,
   [{:test2, {:a1, :a23}, 12, :a1, :a23},
    {:test2, {:a1, :a25}, 16, :a1, :a25}]}
  """
  @spec minus(Relval.t, Relval.t) :: Relval.t
  def minus(left, right) do
    Relval.minus(left, right)
  end
  @doc """
  iex> R.t(fn() ->
  iex>   intersect(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{12, :a1, :a23},
  ...>                             {14, :a1, :a24},
  ...>                             {16, :a1, :a25}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2}),
  ...>         Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{10, :a1, :a22},
  ...>                             {14, :a1, :a24}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2})) |> 
  ...>   Relval.execute() |>
  ...>   Enum.sort()
  ...> end)
  {:atomic,
   [{:test2, {:a1, :a24}, 14, :a1, :a24}]}
  """
  @spec intersect(Relval.t, Relval.t) :: Relval.t
  def intersect(left, right) do
    Relval.intersect(left, right)
  end
  @doc """
  where relval, binding, expression
  expression: Attributename op exp
  op: '==' | '>=' | '<=' | '!=' | '>' | '<'

  example

  iex> R.t(fn() ->
  ...>   where(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                      body: [{12, :a1, :a23},
  ...>                             {14, :a1, :a24},
  ...>                             {16, :a1, :a25}],
  ...>                      keys: [:id, :id2],
  ...>                      name: :test2}), [],
  ...>         value == 14) |>
  ...>   Relval.execute() |>
  ...>   Enum.sort()
  ...> end)
  {:atomic,
   [{:test2, {:a1, :a24}, 14, :a1, :a24}]}
  """
  defmacro where(left, binding \\ [], exp) do
    quote do
      Relval.where(unquote(left), unquote(binding), unquote(exp))
    end
  end
  @doc """
  iex> R.t(fn() ->
  ...>   p = Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                    body: [{12, :a1, :a23},
  ...>                           {14, :a1, :a24},
  ...>                           {16, :a1, :a25}],
  ...>                    keys: [:id, :id2],
  ...>                    name: :test2})
  ...>   p[{:id, :value}] |>
  ...>   Relval.execute() |>
  ...>   Enum.sort()
  ...> end)
  {:atomic, 
    [{:test2, {:a1, 12}, :a1, 12}, 
     {:test2, {:a1, 14}, :a1, 14}, 
     {:test2, {:a1, 16}, :a1, 16}]}
  """
  def project(left, attributes, bool \\ true) when is_list(attributes) do
    Relval.project(left, attributes, bool)
  end
  @doc """
  get fields from relational value

  iex> R.t(fn() ->
  ...>   p = Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                    body: [{12, :a1, :a23},
  ...>                           {14, :a1, :a24},
  ...>                           {16, :a1, :a25}],
  ...>                    keys: [:id, :id2],
  ...>                    name: :test2})
  ...>   fields(p)
  ...> end)
  {:atomic, [:value, :id, :id2]}
  """
  def fields(left) do
    Keyword.keys(left.types)
  end

  @doc """
  join relational values
  
  iex> R.t(fn() ->
  ...>   p = Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                    body: [{12, :a1, :a23},
  ...>                           {14, :a1, :a24},
  ...>                           {16, :a1, :a25}],
  ...>                    keys: [:id, :id2],
  ...>                    name: :test2})
  ...>   q = Relval.new(%{types: [id: :atom, name: :atom],
  ...>                    body: [{:a1, :atom1},
  ...>                           {:a2, :atom2}],
  ...>                    keys: [:id],
  ...>                    name: :test3})
  ...>   join(p, q) |> Relval.execute() |> Enum.sort()
  ...> end)
  {:atomic, 
   [{:test2_test3, {:a1, :a23}, 12, :a1, :a23, :atom1},
    {:test2_test3, {:a1, :a24}, 14, :a1, :a24, :atom1},
    {:test2_test3, {:a1, :a25}, 16, :a1, :a25, :atom1}]}
  """
  def join(left, right) do
    Relval.join(left, right)
  end

  @doc """
  semi join 
  matching(left, right) equivalent to 
    project(join(left, right), fields(left))

  iex> R.t(fn() ->
  ...>   p = Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
  ...>                    body: [{12, :a1, :a23},
  ...>                           {14, :a1, :a24},
  ...>                           {16, :a1, :a25}],
  ...>                    keys: [:id, :id2],
  ...>                    name: :test2})
  ...>   q = Relval.new(%{types: [id2: :atom, name: :atom],
  ...>                    body: [{:a23, :atom1},
  ...>                           {:a24, :atom2}],
  ...>                    keys: [:id2],
  ...>                    name: :test3})
  ...>   matching(p, q) |> Relval.execute() |> Enum.sort()
  ...> end)
  {:atomic, 
   [{:test2, {:a1, :a23}, 12, :a1, :a23},
    {:test2, {:a1, :a24}, 14, :a1, :a24}]}
  """
  def matching(left, right) do
    Relval.matching(left, right)
  end
  @doc """
  divide by
  divide 
  ### example (from Database in Depth: Relational Model for Practitioners's sample model)
  
  iex> R.t(fn() ->
  ...>   sp = Relval.new(%{types: [sno: :atom, pno: :atom, qty: :odd],
  ...>                    body: [{:s1, :p1, 300},
  ...>                           {:s1, :p2, 200},
  ...>                           {:s1, :p3, 400},
  ...>                           {:s1, :p4, 200},
  ...>                           {:s1, :p5, 100},
  ...>                           {:s1, :p6, 100},
  ...>                           {:s2, :p1, 300},
  ...>                           {:s2, :p2, 400},
  ...>                           {:s3, :p2, 200},
  ...>                           {:s4, :p2, 200},
  ...>                           {:s4, :p4, 300},
  ...>                           {:s4, :p5, 400}],
  ...>                    keys: [:sno, :pno],
  ...>                    name: :sp})
  ...>   p = Relval.new(%{types: [pno: :atom, pname: :atom, color: :atom],
  ...>                    body: [{:p1, :nut, :red},
  ...>                           {:p2, :bolt, :green},
  ...>                           {:p3, :screw, :blue},
  ...>                           {:p4, :screw, :red},
  ...>                           {:p5, :cam, :blue},
  ...>                           {:p6, :cog, :red}],
  ...>                    keys: [:pno],
  ...>                    name: :p})
  ...>   divideby(project(sp, [:sno, :pno]), project(p, [:pno])) |> 
  ...>            Relval.execute() |> Enum.sort()
  ...> end)
  {:atomic, 
   [{:sp, :s1, :s1}]}
  """
  def divideby(left, right) do
    fl = fields(left)
    fr = fields(right)
    fd = fl -- fr
    pleft = project(left, fd)
    pr = join(pleft, right)
    p2pr = project(pr, fl)
#    IO.inspect [p2pr: Relval.execute(p2pr), 
#                left: Relval.execute(left)]
    prn = minus(p2pr, left)
#    IO.inspect [prn_minus_p2pr_left: Relval.execute(prn)]

    pprn = project(prn, fd)

    minus(pleft, pprn)

  end
  def rename(left, namelist) when is_list(namelist) do
    newtype = Enum.map(left.type, fn({k, t}) ->
      case Keyword.get(namelist, k) do
        nil -> 
          {k, t}
        v ->
          {v, t}
      end
    end)
    newkey = Enum.map(left.keys, fn(k) ->
      case Keyword.get(namelist, k) do
        nil -> 
          k
        v ->
          v
      end
    end)
    %{left | :type => newtype, :keys => newkey}
  end
  def extend(left, f, [{a, t}]) do
    Relval.extend(left, f, [{a, t}]) 
  end
  
  defmacro with2(fun, a) do
    quote bind_quoted: [fun: fun, a: a] do
      a = fun.()
    end
  end
  @doc """
  relational update

  it is equivalent to:
  u = ((s = where(left, wfun)) |> extend_add(sfun_list))
  left = union(minus(left, s), u)
  """
  defmacro update(bind, do: x) do
    quote do 
      Relval.update(unquote(bind), do: unquote(x))
    end
  end
  defmacro assign(bind, block) do
    quote bind_quoted: [bind: bind, block: block] do
      Relval.assign(bind, block)
    end
  end
  def get(t, key, default \\ nil) do
    case fetch(t, key) do
      :error -> default
      {:ok, value} -> value
    end
  end
  def get_and_update(t, key, f) do
    case f.(get(t, key)) do
      :pop ->
        pop(t, key)
      {old, new} ->
        {old, new}
    end
  end
  def pop(t, key) when is_tuple(key) do
    sels = Tuple.to_list(key)
    atts = Keyword.keys(t.types)
    sels = atts -- sels
    case sels do
      [] -> :error
      _x -> {:ok, project(t, sels)}
    end
  end
  def fetch(t, key) when is_tuple(key) do
    sels = Tuple.to_list(key)
    atts = Keyword.keys(t.types)
    case sels -- atts do
      [] -> {:ok, project(t, sels)}
      _x -> :error
    end
  end

end
