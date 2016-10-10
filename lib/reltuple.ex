defmodule Reltuple do
  @behaviour Access
  defstruct tuple_index: %{}, tuple: {}, types: []
  def new(val, types) do
    IO.inspect [module: :Reltuple, val: val, types: types]
    %__MODULE__{tuple_index: Enum.with_index(Keyword.keys(types)), 
      tuple: val,
      types: types}
  end
  def fetch(%__MODULE__{tuple_index: i, tuple: t}, key) do
    {:ok, elem(t, i[key])}
  end
  def get(%__MODULE__{tuple_index: i, tuple: t}, key, default) do
    case i[key] do
      nil -> default
      r -> elem(t, r)
    end
  end
  def get_and_update(%__MODULE__{tuple_index: i, tuple: t} = v, key, fun) do
    j = i[key]
    {old, j} = case j do
            nil -> {key, nil}
            j -> {elem(t, j), j}
          end
    IO.inspect [j: j]
    {nk, nv}  = case fun.(old) do
      :pop -> 
        {old, nil}
      {old, new} ->
        {old, new}
    end
    case j do
      nil -> 
        {old, %__MODULE__{ v | :tuple => Tuple.append(t, nv)}}
      j -> 
        {old, %__MODULE__{ v | :tuple => put_elem(t, j, nv) }}
    end
  end
  def pop(%__MODULE__{tuple_index: i, tuple: t} = v, key) do
    r = i[key]
    old = case r do
            nil -> nil
            r -> elem(t, r)
          end
    {old, %__MODULE__{ v | :tuple => put_elem(t, r, nil) } }
  end
  def take(v, keys) do
    r = Enum.map(keys, fn(x) ->
      elem(v.tuple, v.tuple_index[x])
    end) 
    |> List.to_tuple 
    |> __MODULE__.new(Keyword.take(v.types, keys))
  end
  def equal?(left, right) do
    Keyword.equal?(left.tuple_index, right.tuple_index) &&
    left.tuple === right.tuple 
  end

end
