defmodule ViewTest do
  use ExUnit.Case, async: false
  require Qlc
  use InitialD
  alias Relvar, as: R
  alias Relval, as: L
  require Relvar
  require Relval
  require InitialD.Relval
  require Reltype
  require View
  @moduletag :view
#  @key :_key
  setup_all do
    :mnesia.start
    Reltype.init()
    Constraint.init()
    View.init()
    on_exit fn ->
      Constraint.destroy()
      
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
      definition: fn(x) -> 
        rem(x, 2) == 0 
      end)
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
  def create_type do
#    keys = [:id]
#    types = [id: :atom, value: :odd]
    R.create(:test2, [:id], [id: :atom, value: :odd])
    R.create(:test22, [:value], [value: :odd, mark: :atom])
    {:atomic, :ok} = :mnesia.transaction(fn() ->
      :mnesia.write({:test2, :id2, :id2, 2} )
      :mnesia.write({:test2, :id4, :id4, 4} )
      :mnesia.write({:test2, :id8, :id8, 4} )
      :mnesia.write({:test2, :id3, :id3, 8} )
      :mnesia.write({:test22, 2, 2, :two} )
      :mnesia.write({:test22, 4, 4, :four} )
      :mnesia.write({:test22, 8, 8, :eight} )
    end)
  end
  test "create/3" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
    end
    assert {:atomic, b} = R.t(fn() ->
      b = L.project(R.to_relvar(:test2), [:id, :value]) |>
        L.execute()
#      IO.inspect [b: b]
      b
    end)
    assert {:atomic, a} = R.t(fn() ->
      a = L.project(R.to_relvar(:test22), [:value, :mark]) |>
        L.execute()
#      IO.inspect [a: a]
      a
    end)
    assert {:atomic, m} = R.t(fn() ->
      m = View.create(:vtest2, 
      quote do
        fn() ->
          require L
          L.join(R.to_relvar(:test2), R.to_relvar(:test22)) |> L.where((value == 4))
        end
      end)
      m
    end)
    {:atomic, m2}  = R.t(fn() -> View.to_relvar(:vtest2) end)
#    IO.inspect [m2: m2, m: m, a: a, b: b]
    assert {:atomic, 
            [ {:test2_test22, :id4, :id4, 4, :four},
              {:test2_test22, :id8, :id8, 4, :four} ]} == R.t(fn() ->
      :mnesia.delete({:test2, :id2})
      L.execute(m2) |> Enum.sort()
    end)
  end
end
