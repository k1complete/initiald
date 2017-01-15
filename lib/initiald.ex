defmodule InitialD do
  @doc """
   deftype :qpr, [q: list(integer)],
                 [constraint: fn(x) -> %x(:aa)]
  """
#  @spec (list +union+ term) :: maybe_improper_list
  ##
  ## row is Dict(columnname, value)
  ## table is HashSet(row)
  require Relval
  require Reltype
  require Relvar2

  @spec union(Relval.t, Relval.t) :: Relval.t
  def union(left,right) do
    Relval.union(left, right)
  end

  @spec minus(Relval.t, Relval.t) :: Relval.t
  def minus(left, right) do
    Relval.minus(left, right)
  end
  @spec intersect(Relval.t, Relval.t) :: Relval.t
  def intersect(left, right) do
    Relval.intersect(left, right)
  end
  defmacro where(left, binding \\ [], exp) do
    Relval.where(left, binding, exp)
  end
  def project(left, attributes, bool \\ true) when is_list(attributes) do
    Relval.project(left, attributes, bool)
  end
  def fields(left) do
    Enum.at(left, 0) |> Map.keys 
  end
  def join(left, right) do
    Relval.join(left, right)
  end
  def matching(left, right) do
    Relval.matching(left, right)
  end
  def divideby(left, right) do
    fl = fields(left)
    fr = fields(right)
    fd = fl -- fr 
    pleft = project(left, fd)
    pr = join(pleft, right)
    minus(pleft, 
          project(minus(pr, left), fd))
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
  @doc """
  relational insert 

  it is equivalent to:
  left = union(left, right)
  """
  def insert(left, right) do
    write!(left, right)
  end
  @doc """
  relational delete assignment

  it is equivalent to:

  left = where(left, &(not(wfun.(&1))))
  """
  def delete(left) do
    Relval.delete(left)
  end
  @doc """
  delete d from left
  """
  def delete!(left, _d) do
#    IO.inspect [delete: left]
    left
  end
  @doc """
  update or insert to left
  """
  def write!(left, _u) do
#    IO.inspect [write: left]
    left
  end

end
