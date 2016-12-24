defmodule ReltupleTest do

  use ExUnit.Case, async: false
  alias Relval, as: L
  alias Reltuple, as: T
  alias Relvar2, as: R
  require Reltuple
  @moduletag :tt

  test "tuple_read_access" do
    val = L.new(%{types: [id: :atom, value: :odd],
                  keys: [:id],
                  body: [{:four, 4}],
                  name: nil})
    R.t(fn() ->
      [t] =  Enum.map(val.query, &(T.new(&1, val.types)))
      #    IO.inspect [t: t]
      assert t[:id] == :four
      assert t[:value] == 4
    end)
  end
  test "tuple_access" do
    val = L.new(%{types: [id: :atom, value: :odd], body: [{:four, 4}], keys: [:id], name: nil})
    val2 = L.new(%{types: [id: :atom, value: :odd], body: [{:five, 5}], keys: [:id], name: nil})
    R.t(fn() ->
      [t] = Enum.map(val.query, &(T.new(&1, val.types)))
      [tf] = Enum.map(val2.query, &(T.new(&1, val2.types)))
      #    IO.inspect [t: t]
      tm = put_in t[:id], :five
      #    IO.inspect [tm: tm]
    
      assert tm[:id] == tf[:id]
      tn = put_in tm[:value], 5
      assert tn == tf
    end)
  end
  test "tuple_take" do
    R.t(fn() ->
    val = L.new(MapSet.new([four: 4]),
                [id: :atom, value: :odd])
    [t] = Enum.map(val.body, &(T.new(&1, val.types)))
#    IO.inspect [t: t]
    assert T.take(t, [:id]) == T.new({:four}, [id: :atom])
    end)
  end
end
    
