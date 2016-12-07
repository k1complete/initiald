defmodule Reltuple do
  @behaviour Access
  defstruct tuple_index: %{}, tuple: {}, types: []
  @type t :: %__MODULE__{tuple_index: map, tuple: tuple, types: keyword}
  @spec new(tuple, [{atom, any}]) :: %__MODULE__{}
  def new(val, types) do
#    IO.inspect [module: :Reltuple, val: val, types: types]
    %__MODULE__{tuple_index: Enum.with_index(Keyword.keys(types)) |> Map.new(), 
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
  @spec get_and_update(%__MODULE__{}, any, (any -> {any, any} | :pop)) :: {any, nil| [any()] | map() | %__MODULE__{}}
  def get_and_update(%__MODULE__{tuple_index: i, tuple: t} = v, key, fun) do
    j = i[key]
    {old, j} = case j do
            nil -> {key, nil}
            j -> {elem(t, j), j}
          end
#    IO.inspect [j: j]
    {_nk, nv}  = case fun.(old) do
      :pop -> 
        {old, nil}
      {old, new} ->
        {old, new}
    end
    newtuple = case j do
                 nil -> 
                   Tuple.append(t, nv)
                 j -> 
                   put_elem(t, j, nv)
               end
    {old, %__MODULE__{ :tuple_index => v.tuple_index,
                       :types => v.types,
                       :tuple => newtuple }}
  end
  @spec pop(%__MODULE__{}, any) :: {any, nil| [any()] | map() | %__MODULE__{}}
  def pop(%__MODULE__{tuple_index: i, tuple: t} = v, key) do
    r = i[key]
    old = case r do
            nil -> nil
            r -> elem(t, r)
          end
    {old, %__MODULE__{ :tuple_index => v.tuple_index,
                       :types => v.types,
                       :tuple => put_elem(t, r, nil) }}
  end
  @spec take(%__MODULE__{}, any) ::  %__MODULE__{}
  def take(%__MODULE__{} = v, keys) do
    Enum.map(keys, fn(x) ->
      elem(v.tuple, v.tuple_index[x])
    end) 
    |> List.to_tuple 
    |> __MODULE__.new(Keyword.take(v.types, keys))
  end
  def equal?(left, right) do
    Map.equal?(left.tuple_index, right.tuple_index) &&
    left.tuple === right.tuple 
  end

end
