defmodule Qpr do
@moduledoc """
   deftype :qpr, [q: list(integer)],
                 [constraint: fn(x) when is_list(x) -> 
                                Enum.all?(x, &(is_integer(&1) and 0 < &1 and
                                10 > &1))
                              end]
"""
  @behaviour Access
  defstruct q: nil
  @type q :: integer()
  def constraint(v) when is_map(v) do
    fn (h) -> 
      is_integer(h.q) and h.q >= 0 and h.q <= 5000
    end.(v)
  end
  def get(qpr, key) when key in [:q] do
    Map.get(qpr, key)
  end
  def get_and_update(qpr, key, fun) when key in [:q] do
    d = Map.get(qpr, key)
    {d, Map.put(qpr, key, fun.(d))}
  end
end
defimpl Collectable, for: Qpr do
  def into(original) do
    {original, 
     fn
       qpr, {:cont, {k, v}} -> 
       case k do
         :q when is_integer(v) and v >= 0 and v <= 5000 -> 
           :maps.put(k, v, qpr)
         _ ->
           raise ArgumentError, "constraint violation #{v}"
       end
       qpr, :done -> qpr
       _, :halt -> :ok
     end}
  end
end
  
defmodule InitialD do
  @doc """
   deftype :qpr, [q: list(integer)],
                 [constraint: fn(x) -> %x(:aa)]
  """
#  @spec (list +union+ term) :: maybe_improper_list
  ##
  ## row is Dict(columnname, value)
  ## table is HashSet(row)
  def union(left,right) do
    Set.union(left, right)
  end
  def minus(left, right) do
    Set.difference(left, right)
  end
  def intersect(left, right) do
    Set.intersection(left, right)
  end
  def where(left, f) do
    Stream.filter(left, f) |> Enum.into(HashSet.new)
  end
  def project(left, attributes, bool \\ true) when is_list(attributes) do
    Stream.map(left, fn(x) ->
                       {t_set, f_set} = Dict.split(x, attributes)
                       if bool do
                         t_set
                       else
                         f_set
                       end
               end) |> Enum.into(HashSet.new)
  end
  def fields(left) do
    Enum.at(left, 0) |> Map.keys 
  end
  def fnjoin(left, right) do
    lkset = fields(left) |> Enum.into(HashSet.new)
    rkset = fields(right) |> Enum.into(HashSet.new)
    ks = Set.intersection(lkset, rkset)
    Enum.reduce(left, 
                [],
                fn(x, acc) ->
                  z = Enum.filter_map(right,
                                      fn(y) ->
                                        Dict.equal?(Map.take(y, ks),
                                                    Map.take(x, ks))
                                      end,
                                      fn(y) ->
                                          Map.merge(x, y)
                                      end)
                    case z do
                      [] -> acc
                      [e] -> 
                        [e|acc]
                    end
                end)
  end
  defp product(left, right) do
    Enum.reduce(left, [], 
                fn(x, acc) -> 
                  Enum.map(right, &(Dict.merge(x, &1))) |> 
                    Enum.into(acc)
                end)
  end
  def join(left, right) do
    if (Enum.count(left) == 0 or Enum.count(right) == 0) do
      HashSet.new()
    else
      fnjoin(left, right) |> Enum.into(HashSet.new)
    end
  end
  def matching(left, right) do
    if (Enum.count(left) == 0 or Enum.count(right) == 0) do
      HashSet.new()
    else
      lkset = fields(left)
      fnjoin(left, right) |> project(lkset)
    end
  end
  def divideby(left, right) do
    fl = fields(left)
    fr = fields(right)
    fd = fl -- fr 
    pleft = project(left, fd)
    pr = Enum.into(product(pleft, right), HashSet.new)
    minus(pleft, 
          project(minus(pr, left), fd))
  end
  def rename(left, namelist) when is_list(namelist) do
    Enum.reduce(left, HashSet.new, 
                fn(x, a) ->
                  r = Enum.reduce(namelist, x,
                                  fn({from, to}, ea) ->
                                    v = Dict.get(ea, from)
                                    Dict.delete(Dict.put(ea, to, v), from)
                                  end)
                  Set.put(a, r)
                end)
  end
  def extend_add(left, alist) when is_list(alist) do
    Enum.reduce(left, HashSet.new,
                fn(e, a) ->
                  Enum.reduce(alist, a, 
                              fn({key, fun}, s) ->
                                Set.put(s, Dict.put(e, key, fun.(e)))
                              end)
                end)
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
  def update(left, where: wfun, set: sfun_list) do
    where(left, wfun) |> extend_add(sfun_list) |> write!(left)
  end
  def update(left, right) do
    minus(right, left) |> write!(left)
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
  def delete(left, where: wfun) do
    where(left, wfun) |> delete!(left)
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
