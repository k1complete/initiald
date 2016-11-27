defmodule Relational_Test3 do
  use ExUnit.Case, async: false
  require Qlc
  require Relvar2
  require Relval
  require Reltype
  alias Relvar2, as: R
  alias Relval, as: L
  @moduletag :test5

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
  def create_type do
    keys = [:id, :id2]
    types = [id: :atom, value: :odd, id2: :atom]
    on_exit fn ->
      R.drop(:test2)
    end
    assert(struct(R, %{:keys => keys, 
                       :name => :test2,
                       :types => types,
                       :attributes => [:_key, :id, :value, :id2]
        })
           == R.create(:test2, [:id, :id2], [id: :atom, value: :odd, id2: :atom]))
    :mnesia.add_table_index(:test2, :value)
    assert({:atomic, :ok} == 
      R.t(fn() ->
        R.write(:test2, {:atom1, 2, :a1})
      end))
    assert({:atomic, :ok} == 
      R.t(fn() ->
        R.write(:test2, {:atom2, 4, :a2})
      end))
    assert({:aborted, 
            {:typecheck_error, :test2, 
             %{value: {:odd, 1}, id: {:atom, 1}}}} == 
      R.t(fn() ->
        R.write(:test2, {1, 1, :a3})
      end))
    assert({:aborted, 
            {:typecheck_error, :test2, 
             %{value: {:odd, 31}}}} == 
      R.t(fn() ->
        R.write(:test2, {:atom3, 31, :a4})
      end))
  end
  test "create_type" do
    create_type()
  end
  @tag :where2
  test "Relational operator where" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.where(relvar, (value == 4)) |> L.execute
    end) == {:atomic,L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2}) |> L.execute
            }

    assert R.t(fn() ->
      L.where(relvar, ({id, id2} == {:atom2,:a2})) |> L.execute
    end) == {:atomic,L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2}) |> L.execute
            }

  end
  test "union" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id: :atom, value: :odd, id2: :atom],
                  body: [{:atom2, 4, :a2}],
                  keys: [:id, :id2],
                  name: :test2})
#      IO.inspect(m: m)
      m2 = L.union(relvar, m)
      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom1, 2, :a1},{:atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2})) |> Enum.sort()
            }
  end
  @tag :intersect2
  test "intersect2" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id: :atom, value: :odd, id2: :atom],
                  body: [{:atom2, 4, :a2}],
                  keys: [:id, :id2],
                  name: :test2})
#      IO.inspect(m: m)
      m2 = L.intersect(relvar, m)
      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2})) |> Enum.sort()
            }
    
  end
  test "minus2" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id: :atom, value: :odd, id2: :atom],
                  body: [{:atom2, 4, :a2},
                         {:atom3, 6, :a3}],
                  keys: [:id, :id2],
                  name: :test2})
#      IO.inspect(m: m)
      m2 = L.minus(relvar, m)
      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom1, 2, :a1}],
              keys: [:id, :id2],
              name: :test2})) |> Enum.sort()
            }
    
  end
  test "join2" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id2: :atom, value: :odd, id3: :atom],
                  body: [{:a2, 4, :a22}],
                  keys: [:id2, :id3],
                  name: :test3})
#      IO.inspect(m: m)
      m2 = L.join(relvar, m)
      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [id: :atom, value: :odd, id2: :atom, id3: :atom],
              body: [{:atom2, 4, :a2, :a22}],
              keys: [:id, :id2, :id3],
              name: :test2_test3})) |> Enum.sort()
            }
    
  end
  test "matching2 semijoin" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id2: :atom, value: :odd, id3: :atom],
                  body: [{:a2, 4, :a22}],
                  keys: [:id2, :id3],
                  name: :test3})
#      IO.inspect(m: m)
      m2 = L.matching(relvar, m)
      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2})) |> Enum.sort()
            }
    
  end
  @tag :project2
  test "project2" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id2: :atom, value: :odd, id3: :atom],
                  body: [{:a2, 4, :a22}],
                  keys: [:id2, :id3],
                  name: :test3})
#      IO.inspect(m: m)
      m2 = L.project(relvar, {value, id2})
      q2 = Qlc.q("[X || X <- Q]", [Q: m2.query])
      IO.inspect [m2: m2]
      m3 = L.where(m2, [], id2 == a2)
      IO.inspect [q2: Qlc.e(q2)]
      IO.inspect [m3: Qlc.e(m3.query)]
      L.execute(m3)|>  Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [value: :odd, id2: :atom],
              body: [{4, :a2}],
              keys: [:value, :id2],
              name: :test2})) |> Enum.sort()
            }
    
  end
  @tag :join3
  test "join3" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
      m = L.new(%{types: [id2: :atom, value: :odd, id3: :atom],
                  body: [{:a2, 4, :a22}],
                  keys: [:id2, :id3],
                  name: :test3})
#      IO.inspect(m: m)
      m2 = L.join(relvar, m) |> L.project({value, id2, id3})
      q2 = Qlc.q("[X || X <- Q]", [Q: m2.query])
      IO.inspect [m2: m2]
      m3 = L.where(m2, [], id2 == a2)
      IO.inspect [q2: Qlc.e(q2)]
      L.execute(m3)|>  Enum.sort()
    end) == {:atomic,
             L.execute(L.new(%{types: [value: :odd, id2: :atom, id3: :atom],
              body: [{4, :a2, :a22}],
              keys: [:value, :id2, :id3],
              name: :test2_test3})) |> Enum.sort()
            }
    
  end
end
