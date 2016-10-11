defmodule ReltupleTest do

  use ExUnit.Case, async: false
  alias Relval2, as: L
  alias Reltuple, as: T
  require Reltuple
  @moduletag :tt

  test "tuple_read_access" do
    val = L.new(MapSet.new([four: 4]),
                [id: :atom, value: :odd])
    [t] = Enum.map(val.body, &(T.new(&1, val.types)))
#    IO.inspect [t: t]
    assert t[:id] == :four
    assert t[:value] == 4
  end
  test "tuple_access" do
    val = L.new(MapSet.new([four: 4]),
                [id: :atom, value: :odd])
    val2 = L.new(MapSet.new([five: 5]),
                [id: :atom, value: :odd])
    [t] = Enum.map(val.body, &(T.new(&1, val.types)))
    [tf] = Enum.map(val2.body, &(T.new(&1, val2.types)))
#    IO.inspect [t: t]
    tm = put_in t[:id], :five
#    IO.inspect [tm: tm]
    
    assert tm[:id] == tf[:id]
    tn = put_in tm[:value], 5
    assert tn == tf
  end
  test "tuple_take" do
    val = L.new(MapSet.new([four: 4]),
                [id: :atom, value: :odd])
    [t] = Enum.map(val.body, &(T.new(&1, val.types)))
#    IO.inspect [t: t]
    assert T.take(t, [:id]) == T.new({:four}, [id: :atom])
  end
end
    
