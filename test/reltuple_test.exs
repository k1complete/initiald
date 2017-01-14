defmodule ReltupleTest do

  use ExUnit.Case, async: false
  alias Relval, as: L
  alias Reltuple, as: T
  alias Relvar2, as: R
  require Reltuple
  require Reltype
  @moduletag :tt
  setup_all do
    :mnesia.start
    Reltype.init
    Constraint.init
    on_exit fn() ->
      Constraint.destroy
    end
    :ok
  end
  setup do
    m = Reltype.reltype(typename: :atom, 
                        definition: fn(x) -> is_atom(x) end,
    cast: fn(x) when is_atom(x) -> x
            (x) when is_binary(x) -> String.to_atom(x) 
          end
    )
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :odd, 
                        definition: fn(x) -> rem(x, 2) == 0 end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    on_exit fn ->
#      IO.puts "destroy"
      R.t(fn ->
        Reltype.delete(:atom)
        Reltype.delete(:odd)
      end)
    end
    :ok
  end

  test "tuple_read_access" do
    {:atomic, val}  = R.t(fn() -> 
      L.new(%{types: [id: :atom, value: :odd],
              keys: [:id],
              body: [{:four, 4}],
              name: nil})
    end)
    R.t(fn() ->
      [t] =  Enum.map(val.query, &(T.new(&1, val.types)))
      #    IO.inspect [t: t]
      assert t[:id] == :four
      assert t[:value] == 4
    end)
  end
  test "tuple_access" do
    {:atomic, val} = R.t(fn() -> 
      L.new(%{types: [id: :atom, value: :odd], 
              body: [{:four, 4}], keys: [:id], name: nil}) 
      end)
    {:atomic, val2} = R.t(fn() ->
      L.raw_new(%{types: [id: :atom, value: :odd], 
              body: [{:five, 5}], keys: [:id], name: nil})
    end)
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
    
