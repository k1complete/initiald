defmodule RelationalTest do
  use ExUnit.Case, async: false
  require Qlc
  use InitialD
  alias Relvar, as: R
  alias Relval, as: L
  require Reltype
  require Relvar
  require Relval
  @moduletag :test_rel

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
    m = Reltype.reltype(typename: :atom, definition: fn(x) -> is_atom(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :odd, definition: fn(x) -> rem(x, 2) == 0 end)
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
    keys = [:id]
    types = [id: :atom, value: :odd]
    on_exit fn ->
      R.drop(:test2)
    end
    R.create(:test2, [:id], [id: :atom, value: :odd])
    assert({:atomic, :ok} == 
      R.t(fn() ->
        R.write(:test2, {:atom1, 2})
      end))
    assert({:atomic, :ok} == 
      R.t(fn() ->
        R.write(:test2, {:atom2, 4})
      end))
    assert({:aborted, 
            {:typecheck_error, :test2, 
             %{value: {:odd, 1}, id: {:atom, 1}}}} == 
      R.t(fn() ->
        R.write(:test2, {1, 1})
      end))
    assert({:aborted, 
            {:typecheck_error, :test2, 
             %{value: {:odd, 31}}}} == 
      R.t(fn() ->
        R.write(:test2, {:atom3, 31})
      end))
  end
  test "create_type" do
    create_type()
  end
  test "read_table" do
    create_type()
    r = R.t(fn() ->
#      IO.inspect [:mnesia.all_keys(:test2)]
      :mnesia.read({:test2, :atom2})
    end)
    assert(r == {:atomic, [{:test2, :atom2, :atom2, 4}]})
    r = R.t(fn() ->
      :mnesia.read({:test2, :atom3})
    end)
    assert(r == {:atomic, []})
    r = R.t(fn() ->
      :mnesia.read({:test_no, :atom2})
    end)
    assert(r == {:aborted, {:no_exists, :test_no}})
    
  end
  test "Enumerable operator member? for Relvar" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval = {:atom1, 2}
    assert R.t(fn() -> Enum.member?(relvar, relval) end)  == {
      :atomic, true}
    relval = {:atom1, 1}
    assert R.t(fn() -> Enum.member?(relvar, relval) end)  == {
      :atomic, false}
  end
  test "create_type_error" do
    keys = [:id]
    types = [id: :atom, value: :odd3]
    on_exit fn ->
      R.drop(:test3)
    end
    assert catch_exit(R.create(:test3, keys, types)) ==
      {:aborted, {:attribute_type_unmatch, 
                  :test3,
                  %{types: types,
                    diff: [:odd3]}}}
  end
  test "Enumerable protocol" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert Enum.count(relvar) == 2
    error_relvar = :t
    assert catch_error(Enum.count(%R{name: error_relvar})) == 
      %ArgumentError{message: "not found relational variable: #{error_relvar}"}
    assert R.t(fn() ->
      Enum.into([a: 4, b: 2], relvar)
    end) == {:atomic, relvar}
    assert Enum.count(relvar) == 4
    
  end
  test "Collectable protocol" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      Enum.into([{:a, 3}, {:b, 2}], relvar)
    end) == {:aborted, 
             {:typecheck_error, :test2,
              %{value: {:odd,  3}}}}
    assert R.t(fn() ->
      Enum.into([{:a, 4}, {:b, 2}], relvar)
    end) == {:atomic, relvar}
    {c, r} =  R.t(fn() ->
      Enum.map(relvar, fn({k, v}) ->
#        IO.inspect [map: {k, v}]
        {k, v}
      end)
    end)
    assert c == :atomic
    assert Enum.sort(r) == Enum.sort([{:a, 4}, {:atom1, 2},
                                      {:atom2, 4}, {:b, 2}])
  end
  test "Relational operator union" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = L.raw_new(%{body: [{:test22, :atom1, :atom1, 2}, 
                                 {:test22, :atom3, :atom3, 6}],
                          types: [id: :atom, value: :odd],
                          keys: [:id],
                          name: :test22
    })
    assert R.t(fn() ->
      L.union(relvar, relval2) |> L.execute()|> Enum.sort()
      end) == {:atomic,L.raw_new(%{body: 
                                   [{:test2, :atom1, :atom1, 2},
                                    {:test2, :atom2, :atom2, 4},
                                    {:test2, :atom3, :atom3, 6}],
                               name: :test2,
                               keys: [:id],
                             types: [id: :atom, value: :odd]}) 
              |> L.execute() |> Enum.sort() }
  end
  test "Relational operator minus" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = L.raw_new(%{types: [id: :atom, value: :odd],
                       body: [{:test22, :atom1, :atom1, 2}],
                      name: :test22,
                      keys: [:id]
    })
    assert R.t(fn() ->
      L.minus(relvar, relval2) |> L.execute()
      end) == {:atomic, L.raw_new(%{body: 
                                    [{:test2, :atom2, :atom2, 4}],
                                    types: [id: :atom, value: :odd],
                                    name: :test2,
                                    keys: [:id]
                }) |> L.execute() }
  end
  test "Relational operator intersect" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = L.raw_new(%{types: [id: :atom, value: :odd],
                          body: [{:test22, :atom1, :atom1, 2}],
                          name: :test22,
                          keys: [:id]
    })
    assert R.t(fn() ->
      L.intersect(relvar, relval2) |> L.execute()
      end) == {:atomic, L.raw_new(%{body: [{:test2, :atom1, :atom1, 2}],
                                types: [id: :atom, value: :odd],
                                name: :test2,
                                keys: [:id]
              }) |> L.execute() }
  end
  test "Relational operator where" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.where(relvar, [], (value == 4)) |> L.execute()
    end) == {:atomic,L.raw_new(%{body: [{:test2, :atom2, :atom2, 4}],
                                 types: [id: :atom, value: :odd],
                                 name: :test2,
                                 keys: [:id]
              }) |> L.execute() }
  end
  test "Relational operator project" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.project(relvar, [:value]) |> L.execute() |> Enum.sort()
    end) == {:atomic, L.raw_new(%{body: [{:test2, 2, 2},{:test2, 4, 4}],
                                  types: [value: :odd],
                                  name: :test2,
                                  keys: [:value]
              }) |> L.execute() |> Enum.sort() }
  end
  test "Relational operator fnjoin" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = L.raw_new(%{body: 
                          [{:test22, {2, :two}, 2, :two}, 
                           {:test22, {4, :four},4, :four}, 
                           {:test22, {6, :six}, 6, :six}],
                          types: [value: :odd, name: :atom],
                          name: :test22,
                          keys: [:value, :name]
    })

    assert R.t(fn() ->
      L.join(relvar,  relval2) |> L.execute() |> Enum.sort()
    end) == {:atomic,L.raw_new(%{body: [{:test2_test22, {:atom1, :two}, 
                                         :atom1, 2, :two},
                                        {:test2_test22, {:atom2, :four},
                                         :atom2, 4, :four}],
                                 types: [id: :atom, value: :odd, name: :atom],
                                 name: :test2_test22,
                                 keys: [:id, :name]
              }) |> L.execute() |> Enum.sort() }
  end
  test "Relational operation rename" do
    create_type()
#    relvar = R.to_relvar(:test2)
#    assert {:atomic, 
#            [{:atom1, 2}, {:atom2, 4}]
#      == R.t(fn() -> L.rename(relvar, :id, :id2) end)
#    assert(
#      {:atomic, 
#       %{%{id2: :atom1} => %{value2: 2}, 
#         %{id2: :atom2} => %{value2: 4}}} ==
#      R.t(fn() -> 
#        L.rename(relvar, :id, :id2) |>
#          L.rename(:value, :value2)
#      end))
  end
  test "Relational operation extend" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert {:atomic, 
            L.raw_new(%{body: [{:test2, :atom1, :atom1, 2, :atom1_1},
                               {:test2, :atom2, :atom2, 4, :atom2_1}],
                        types: [id: :atom, value: :odd, id_sub: :atom],
                        name: :test2,
                        keys: [:id]} 
           ) |> L.execute() |> Enum.sort() } == 
      R.t(fn() -> 
        L.extend(relvar,  fn(t) -> 
          x = t[:id]
          :"#{x}_1"
        end, id_sub: :atom) |> L.execute() |> Enum.sort()
      end)
  end
end
