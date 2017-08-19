alias InitialD.Relval
defmodule InitialD do
  require Relval
#  @behaviour Access
  @doc """
  initialize InitialD 


  """
  defmacro __using__(_ops) do
    quote do
      alias InitialD.Relval
      alias InitialD.Reltype
      alias InitialD.Relvar
      alias InitialD.Reltuple
      alias InitialD.Constraint
      alias InitialD
      alias InitialD.Type
      require Relval
      require Relval.Assign
      require Reltype
      require Relvar
      require InitialD
      require Qlc
      import InitialD
    end
  end
  @doc """
  union left and right

  ## example

      iex> R.t(fn() ->
      ...>   union(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
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
  minus right from left

  ## example

      iex> R.t(fn() ->
      ...>   minus(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
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
  intersect left and right

  ## example

      iex> R.t(fn() ->
      ...>   intersect(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
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

  ## example

      ...> R.t(fn() ->
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
  projection

  ## example

      iex> R.t(fn() ->
      ...>   p = Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
      ...>                    body: [{12, :a1, :a23},
      ...>                           {14, :a1, :a24},
      ...>                           {16, :a1, :a25}],
      ...>                    keys: [:id, :id2],
      ...>                    name: :test2})
      ...>   project(p, [:id, :value]) |>
      ...>   Relval.execute() |>
      ...>   Enum.sort()
      ...> end)
      {:atomic, 
       [{:test2, {:a1, 12}, :a1, 12}, 
        {:test2, {:a1, 14}, :a1, 14}, 
        {:test2, {:a1, 16}, :a1, 16}]}

   shortcut: p[tuple_of_attributes]
 
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

      ...> R.t(fn() ->
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
  @spec join(InitialD.Relval.t, InitialD.Relval.t) :: InitialD.Relval.t
  def join(left, right) do
    Relval.join(left, right)
  end

  @doc """
  semi join 
  matching(left, right) equivalent to 
    project(join(left, right), fields(left))

      ...> R.t(fn() ->
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
  @spec matching(InitialD.Relval.t, InitialD.Relval.t) :: InitialD.Relval.t
  def matching(left, right) do
    Relval.matching(left, right)
  end
  @doc """
  divide by
  divide 

  ## example 

  from Database in Depth: Relational Model for Practitioners's sample model
  
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
      ...>   divideby(sp[{:sno, :pno}], p[{:pno}]) |> 
      ...>            Relval.execute() |> Enum.sort()
      ...> end)
      {:atomic, 
       [{:sp, :s1, :s1}]}
  """
  @spec divideby(InitialD.Relval.t, InitialD.Relval.t) :: InitialD.Relval.t
  def divideby(left, right) do
    fl = fields(left)
    fr = fields(right)
    fdl = fl -- fr
    fdt = List.to_tuple(fdl)
    flt = List.to_tuple(fl)
    # pleft = left[fdt]
    # pr = join(left[fdt], right)
    # p2pr = pr[flt]
    # p2pr = join(left[fdt], right)[flt]
    # project(pr, fl)
    # prn = minus(p2pr, left)
    # pprn = project(prn, fdl)
    # minus(pleft, pprn)
    minus(left[fdt], 
          minus(join(left[fdt], right)[flt], 
                left)[fdt])

  end
  @doc """
  rename relval's attributes follow keyword list oldname to newname.
  this is not ddl(ALTER TABLE ... RENAME .. in SQL).
 
      iex> R.t(fn() ->
      ...>   sp = Relval.new(%{types: [sno: :atom, pno: :atom, qty: :odd],
      ...>                    body: [{:s1, :p1, 300},
      ...>                           {:s1, :p2, 200}],
      ...>                    keys: [:sno, :pno],
      ...>                    name: :sp})
      ...>  rename(sp, [pno: :product_number])
      ...> end)
      {:atomic,
        %InitialD.Relval{keys: [:sno, :product_number], name: :sp,
          query: [{:sp, {:s1, :p1}, :s1, :p1, 300},
                  {:sp, {:s1, :p2}, :s1, :p2, 200}],
          types: [sno: :atom, product_number: :atom, qty: :odd]}}
  """
  def rename(left, namelist) when is_list(namelist) do
    newtype = Enum.map(left.types, fn({k, t}) ->
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
    %{left | :types => newtype, :keys => newkey}
  end
  @doc """
  add new attribute from fun
  this is not ddl(ALTER TABLE ... ADD COLUMN .. in SQL).

  ## example

      iex> R.t(fn() ->
      ...>   sp = Relval.new(%{types: [sno: :atom, pno: :atom, qty: :odd],
      ...>                    body: [{:s1, :p1, 300},
      ...>                           {:s1, :p2, 240}],
      ...>                    keys: [:sno, :pno],
      ...>                    name: :sp})
      ...>  extend(sp, fn(r) -> div(r[:qty],12) end, [dars: :odd]) |>
      ...>    Relval.execute() |> Enum.sort()
      ...> end)
      {:atomic,
        [{:sp, {:s1, :p1}, :s1, :p1, 300, 25},
         {:sp, {:s1, :p2}, :s1, :p2, 240, 20}]}
  
  """
  def extend(left, f, [{a, t}]) do
    Relval.extend(left, f, [{a, t}]) 
  end

  @doc """
  relational assingment

  is equivalent to:
  u = ((s = where(left, wfun)) |> extend_add(sfun_list))
  left = union(minus(left, s), u)
##      {:atomic, [{:a1, {:a1, :v1}, :a1, :v1}, 
##                 {:a1, {:a2, :v2}, :a2, :v2}]}

      iex> a = Relvar.create(:a1, [:key], [key: :atom, value: :atom])
      iex> R.t(fn() -> 
      ...>   assign [] do
      ...>     insert: a -> 
      ...>       Relval.new(%{types: [key: :atom, value: :atom],
      ...>                    body: [{:a1, :v1}, {:a2, :v2}],
      ...>                    keys: [:key],
      ...>                    name: :a1})
      ...>   end
      ...>   b = a[{:key, :value}]
      ...>   Relval.execute(b) |> Enum.sort()
      ...> end)
      ...> R.t(fn() ->
      ...>   assign [] do
      ...>     update: where(a, (key == :a1)) -> 
      ...>       [key: :a3, value: old[:value]]
      ...>   end
      ...>   b = a[{:key, :value}]
      ...>   Relval.execute(b) |> Enum.sort()
      ...> end)
      {:atomic, 
        [{:a1, {:a1, :v1}, :a1, :v1}, 
         {:a1, {:a2, :v2}, :a2, :v2}]}

  
  """
  defmacro assign(bind, block) do
    quote do
      Relval.assign(unquote(bind), unquote(block))
    end
  end
end
