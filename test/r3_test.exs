defmodule Relational_Test3 do
  use ExUnit.Case, async: false
  require Qlc
  use InitialD
  require Relvar2
  require Relval
  require Reltype
  alias Relvar2, as: R
  alias Relval, as: L
  require Reltuple
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
    end) == {:atomic,L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:test2, {:atom2, :a2}, :atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2}) |> L.execute
            }

    assert R.t(fn() ->
      L.where(relvar, ({id, id2} == {:atom2,:a2})) |> L.execute
    end) == {:atomic,L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:test2, {:atom2, :a2}, :atom2, 4, :a2}],
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
#      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
                                   body: [{:test2, {:atom1, :a1}, 
                                           :atom1, 2, :a1},
                                          {:test2, {:atom2, :a2}, 
                                           :atom2, 4, :a2}],
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
#      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
                                   body: [{:test2, {:atom2, :a2},
                                           :atom2, 4, :a2}],
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
#      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
                                   body: [{:test2, {:atom1, :a1}, 
                                           :atom1, 2, :a1}],
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
#      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [id: :atom, 
                                           value: :odd, 
                                           id2: :atom, 
                                           id3: :atom],
                                   body: [{:test2_test3, 
                                           {:atom2, :a2, :a22},
                                           :atom2, 4, :a2, :a22}],
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
#      IO.inspect [m: :qlc.info(m.query), m2: m2]
#      :qlc.info(m2.query)
      L.execute(m2)|> Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [id: :atom, value: :odd, id2: :atom],
              body: [{:test2, {:atom2, :a2}, :atom2, 4, :a2}],
              keys: [:id, :id2],
              name: :test2})) |> Enum.sort()
            }
    
  end
  @tag :project2
  test "project2" do
    create_type()
    relvar = R.to_relvar(:test2)
    
    assert R.t(fn() ->
#      m = L.new(%{types: [id2: :atom, value: :odd, id3: :atom],
#                  body: [{:a2, 4, :a22}],
#                  keys: [:id2, :id3],
#                  name: :test3})
#      IO.inspect(m: m)
      m2 = L.project(relvar, [:value, :id2])
#      q2 = Qlc.q("[X || X <- Q]", [Q: m2.query])
#      IO.inspect [m2: m2]
      m3 = L.where(m2, [], id2 == a2)
#      IO.inspect [q2: Qlc.e(q2)]
#      IO.inspect [m3: Qlc.e(m3.query)]
      L.execute(m3)|>  Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: [value: :odd, id2: :atom],
              body: [{:test2, {4, :a2}, 4, :a2}],
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
      m2 = L.join(relvar, m) |> L.project([:value, :id2, :id3])
#      q2 = Qlc.q("[X || X <- Q]", [Q: m2.query])
#      IO.inspect [m2: m2]
      m3 = L.where(m2, [], id2 == a2)
 #     IO.inspect [q2: Qlc.e(q2)]
      L.execute(m3)|>  Enum.sort()
    end) == {:atomic,
             L.execute(L.raw_new(%{types: 
                                   [value: :odd, id2: :atom, id3: :atom],
                                   body: [{:test2_test3, {4, :a2, :a22},
                                           4, :a2, :a22}],
                                   keys: [:value, :id2, :id3],
                                   name: :test2_test3})) |> Enum.sort()
            }
    
  end
  test "tuple" do
    R.t(fn() ->
      new = old = Reltuple.new({:id1, 2, :id2}, [id: :atom, value: :odd, id2: :atom])
      # lfld = fld+exp  --> new = put_in(new[:lfld], old[:fld]+exp)
      # lfld2 = exp      --> new = put_in(new[:lfld2], exp)
      new = put_in(new[:id], :atom)
      new = put_in(new[:id2], old[:id])
      assert(new.tuple == {:atom, 2, :id1})
    end)
  end
  @tag :update2
  test "update_test" do
    create_type()
    relvar = R.to_relvar(:test2)
    s = 3
    R.t(fn() -> 
      :mnesia.write({:test2, {:id3, :id33}, :id3, 2, :id33})
    end)
#    R.t(fn() -> 
#      IO.inspect [test2: Qlc.q("[X || X <- Q]", 
#                               [Q: :mnesia.table(:test2)]) |> Qlc.e()]
#    end)
    assert {:atomic, _} = R.t(fn() ->
      L.update [s: s] do
        L.where(relvar, (value == 2)) ->
#          put_in new[:value], old[:value] * 2
#          put_in new[:id2], old[:id]
          [id2: old[:id], value: old[:value]*2]
#          [OK: 1]
#        L.where(m, (id == :atom2)) ->
##          put_in new, :value, old[:value]+1
#          [id2: true]
      end
#      L.execute(L.project(relvar, [:id, :value, :id2]))
    end) 
    assert {:atomic, _} = R.t(fn() ->
      L.update [s: s] do
        L.where(relvar, (value == 2 or value == 4)) ->
#          IO.inspect [update: old]
          [id2: old[:id], value: old[:value]*2]
      end
    end) 
#    R.t(fn() -> 
#      IO.inspect [test2: Qlc.q("[X || X <- Q]", 
#                               [Q: :mnesia.table(:test2)]) |> Qlc.e()]
#    end)
  end
  @tag :update
  test "update_test_type_constraint_error" do
    create_type()
    relvar = R.to_relvar(:test2)
    s = 2
    assert {:aborted,
            %Reltype.TypeConstraintError{attribute: :value, type: :odd,
                                          value: 3}} == R.t(fn() ->
      L.update [s: s] do
        L.where(relvar, (value == 4)) ->
          [id2: old[:id], value: 3]
      end
    end) 
  end
  @tag :project
  test "project" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert {:atomic, Enum.sort([{:test2, 2, 2},
                                {:test2, 4, 4}])} == 
      R.t(fn() ->
        relvar[{:value}] |> L.execute() |> Enum.sort()
      end)
  end
end
