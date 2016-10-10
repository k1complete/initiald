defmodule Relval do
  alias Relvar, as: R
  require Qlc
  require Logger
  @type key_name :: atom
  @type key_val :: any
  @type key_set :: %{key_name => key_val}
  @type value_set :: %{value_name => value_val}
  @type value_name :: atom
  @type value_val :: any
  @type relval :: %{key_set => value_set}
  @spec union(R.t | relval, R.t | relval) :: relval
  @doc """
  union left and right.

  left and right should have same keys and attributes.
  """
  def union(left, right) do
    Enum.into(left, Enum.into(right, %{}))
  end
  @spec minus(R.t | relval, R.t | relval) :: relval
  def minus(left, right) do
    {_d, ret} = Map.split(Enum.into(left, %{}), 
                          Map.keys(Enum.into(right, %{})))
    ret
  end
  @spec intersect(R.t | relval, R.t | relval) :: relval
  def intersect(left, right) do
    {ret, _d} = Map.split(Enum.into(left, %{}), Map.keys(Enum.into(right, %{})))
    ret
  end
  def record_to_map(r, attributes) do
#    IO.inspect [tuple_to_map: r, att: attributes]
    [_, k|rest] = Tuple.to_list(r)
    [:key|arest] = attributes
    v = Enum.zip(arest, rest)|> Map.new
#    IO.inspect [tuple_to_map: {k,v}]
    {k,v}
  end
  @spec where(R.t | relval, ({key_set, value_set} -> boolean)) :: relval
  def where(left = %R{}, f) do 
#    IO.inspect [left: left.attributes]
    relname = R.table(left)
    att = left.attributes
    qlc = Qlc.q("""
    [{K, V} || X <- Rel, 
               F({K,V} = TM(X, A)) =:= true]
    """, [Rel: relname,
          A: att,
          F: f,
          TM: fn(x,y) -> record_to_map(x, y) end])
    Qlc.e(qlc) |>
      Map.new()
  end
  def where(left, f) do
    Stream.filter(left, f) |> Map.new()
  end
  @spec project(R.t | relval, [value_name|key_name], boolean) :: relval
  def project(left, attributes, take \\ true) do
    Stream.map(left, fn({k, v}) ->
      {take_set, rest_set} = Map.split(Map.merge(k, v), attributes)
      if (take) do
        {take_set, %{}}
      else
        {rest_set, %{}}
      end
    end) |> Map.new()
  end
  @spec fields(R.t|relval) :: list
  def fields(left = %R{}) do
    [:key|att] = left.attributes
    {left.keys, att}
  end
  def fields(left) do
    case Map.keys(left) do
      [] ->
        {[], []}
      [h|_] ->
        v = Map.get(left, h)
        {Map.keys(h), Map.keys(v)}
    end
  end
  def rename(left, old, newname) do
    Enum.map(left, fn({k, v}) ->
      case Map.fetch(k, old) do
        {:ok, value} ->
          {Map.delete(k, old) |> Map.put(newname, value), v}
        :error ->
          case Map.fetch(v, old) do
            {:ok, value} ->
              {k, Map.delete(v, old) |> Map.put(newname, value)}
            :error ->
              {k, v}
          end
      end
    end) |> Map.new
  end
  @doc """
  add name attribute to relval.
  """
  @spec extend(R.t|relval, atom, (key_set, value_set -> any)) :: relval
  def extend(relval, name, f) do
    Enum.map(relval, fn({k,v}) -> 
      {k, Map.put(v, name, f.({k, v}))}
    end)|>Map.new()
  end
  @spec join(R.t|relval, R.t|relval, (map, map, map -> map)) :: map
  def join(left, right, f) do
    {lk, lv} = fields(left)
    {rk, rv} = fields(right)
#    IO.inspect [rk: rk, rv: rv]
    la = MapSet.union(MapSet.new(lk), MapSet.new(lv))
    ra = MapSet.union(MapSet.new(rk), MapSet.new(rv))
    fjkeys = MapSet.intersection(la, ra) |> MapSet.to_list
#    IO.inspect [fjkeys_base: {la, ra}, fjkeys: fjkeys]
    for {lk, lv} <- left, {rk, rv} <- right, 
      {left_comp, left_rest} = Map.split(Map.merge(lk, lv), fjkeys),
      {right_comp, right_rest} = Map.split(Map.merge(rk, rv), fjkeys),
      left_comp == right_comp do
        f.(left_comp, left_rest, right_rest)
    end |> Map.new
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
    join(left, right, fn(left_comp, left_rest, right_rest) ->
      {Map.merge(left_comp, left_rest) |>Map.merge(right_rest), %{}}
    end)
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
  def grouping(e, map, fun) do
    Enum.reduce(e, map, fn(entry, categories) ->
      Map.update(categories, fun.(entry), [entry], 
                 fn %{} ->
                     [entry]
                   x ->
                     [entry|x]
                 end)
    end)
  end
  @spec max(map, value_set, atom, atom) :: map
  def count(m, v, count_label, label) do
    qd = case Map.get(v, count_label) do
           nil -> 0
           _q -> 1
         end
    Map.update(m, label, qd, fn(x) ->
      x + 1
    end)
  end
  
  @spec max(map, value_set, atom, atom) :: map
  def max(m, v, target_label, label) do
    qd = case Map.get(v, target_label) do
           nil -> 0
           q -> q
         end
    Map.update(m, label, qd, fn(x) ->
      case (qd >= x) do
        true -> qd
        false -> x
      end
    end)
  end
  @doc """
  summarize operator

  summarize left per right, calculate and add new tuple value.
  """
  @spec summarize(relval|Relvar.t, relval|Relvar.t, [add: (map, {key_set, value_set} -> map)]) :: relval
  def summarize(left, right, [add: summary_list_fun]) do
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
end
