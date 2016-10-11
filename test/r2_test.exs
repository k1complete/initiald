defmodule RelationalTest3 do
  use ExUnit.Case, async: false
  require Qlc
  alias Relvar2, as: R
  alias Relval2, as: L
  require Relvar2
  require Reltype
  @moduletag :test3

  setup_all do
    :mnesia.start
    Reltype.init
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
    keys = [:id]
    types = [id: :atom, value: :odd]
    on_exit fn ->
      R.drop(:test2)
    end
    assert(struct(R, %{:keys => keys, 
                       :name => :test2,
                       :types => types,
                       :attributes => [:key, :id, :value]
        })
           == R.create(:test2, [:id], [id: :atom, value: :odd]))
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
      :mnesia.read({:test2, :atom2})
    end)
    assert(r == {:atomic, [{:test2, :atom2, :atom2, 4}]})
    r = R.t(fn() ->
      :mnesia.read({:test2, %{:id => :atom3}})
    end)
    assert(r == {:atomic, []})
    r = R.t(fn() ->
      :mnesia.read({:test_no, :atom2})
    end)
    assert(r == {:aborted, {:no_exists, :test_no}})
    
  end
  @tag :r3
  test "Enumerable operator member? for Relvar" do
    create_type()
    relvar = R.to_relvar(:test2)
#    relvale = {:atom1, 2}
    assert R.t(fn() -> Enum.member?(relvar, {:atom1, 2}) end)  == {
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
      Enum.into([{:a, 4},
                 {:b, 2}], relvar)
    end) == {:atomic, relvar}
    assert Enum.count(relvar) == 4
    
  end
  test "Collectable protocol" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      Enum.into([a: 3, b: 2], relvar)
    end) == {:aborted, 
             {:typecheck_error, :test2,
              %{value: {:odd,  3}}}}
    assert R.t(fn() ->
      Enum.into([a: 4, b: 2], relvar)
    end) == {:atomic, relvar}
    {c, r} =  R.t(fn() ->
      Enum.map(relvar, fn({k, v}) ->
#        IO.inspect [map: {k, v}]
        {k, v}
      end)
    end)
    assert c == :atomic
    assert Enum.sort(r) == Enum.sort([atom1: 2,
                                      atom2: 4,
                                      a: 4,
                                      b: 2])
  end
  test "Collectable relval" do
#    IO.inspect "aaa"
    create_type()
#    relvar = R.to_relvar(:test2)
    relval = %L{types: [id: :atom, value: :odd]}
    relval2 = Enum.into(MapSet.new([{:atom1, 2}, {:atom3, 6}]), relval)
#    IO.inspect relval2
    assert relval2.body == MapSet.new([{:atom1, 2}, {:atom3, 6}])
  end
  test "Relational operator union" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = %L{body: MapSet.new([{:atom1, 2},
                                   {:atom3, 6}]),
                 types: [id: :atom, value: :odd]}
    assert R.t(fn() ->
      L.union(relvar, relval2)
      end) == {:atomic,
               %L{body: MapSet.new([{:atom1, 2},
                                    {:atom2, 4},
                                    {:atom3, 6}]),
                  types: [id: :atom, value: :odd]}}
  end
  test "Relational operator minus" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = %L{types: [id: :atom, value: :odd],
                 body: MapSet.new([{:atom1, 2}])}
    assert R.t(fn() ->
      L.minus(relvar, relval2)
    end) == {:atomic, %L{body: MapSet.new([{:atom2, 4}]),
                         types: [id: :atom, value: :odd]}}
  end
  test "Relational operator intersect" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = %L{body: MapSet.new([{:atom1, 2}]),
                 types: [id: :atom, value: :odd]
                }
    assert R.t(fn() ->
      L.intersect(relvar, relval2)
      end) == {:atomic, relval2}
  end
  test "Relational operator where" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.where(relvar, fn(v) -> 
#        IO.inspect [v2: v]
        case v[:value] do
          4 -> true
          _ -> false
        end
      end)
    end) == {:atomic,%L{body: MapSet.new([{:atom2, 4}]),
                        types: [id: :atom, value: :odd]}}
  end
  test "Relational operator project" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.project(relvar, [:value], true)
    end) == {:atomic,%L{body: MapSet.new([{2},{4}]),
                        types: [value: :odd]}}
    assert R.t(fn() ->
      L.project(relvar, [:value], false)
    end) == {:atomic,%L{body: MapSet.new([{:atom1},{:atom2}]),
                        types: [id: :atom]}}

  end
  test "Relational operator project relval" do
    relval2 = %L{body: MapSet.new([{2, :two}, {4, :four}, {0, :zero}]),
                 types: [value: :odd, name: :atom]}
    assert R.t(fn() ->
      L.project(relval2, [:name], true)
    end) == {:atomic,%L{body: MapSet.new([{:two},{:four},{:zero}]),
                        types: [name: :atom]}}
    assert R.t(fn() ->
      L.project(relval2, [:name], false)
    end) == {:atomic,%L{body: MapSet.new([{2},{4},{0}]),
                        types: [value: :odd]}}

  end

  @tag :r2
  test "Relational operator fnjoin" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = %L{body: MapSet.new([{2, :two}, {4, :four}, {0, :zero}]),
                 types: [value: :odd, name: :atom]}
    assert R.t(fn() ->
      L.fnjoin(relvar,  relval2)
    end) == {:atomic,%L{body: MapSet.new([{:atom1, 2, :two},
                               {:atom2, 4, :four}]),
                        types: [id: :atom, value: :odd, name: :atom]}}
  end
  test "Relational operation rename" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert {:atomic, 
            %Relvar2{attributes: [:key, :id, :value], constraints: nil,
             keys: [:id2], name: :test2, types: [id2: :atom, value: :odd]}}
      == R.t(fn() -> L.rename(relvar, :id, :id2) end)
    assert(
      {:atomic, 
       %Relvar2{attributes: [:key, :id, :value], constraints: nil,
                keys: [:id2], name: :test2, 
                types: [id2: :atom, value2: :odd]}} ==
      R.t(fn() -> 
        L.rename(relvar, :id, :id2) |>
          L.rename(:value, :value2)
      end))
  end
  test "Relational operation extend" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert {:atomic, 
            %L{body: MapSet.new([{:atom1, 2, :atom1_1},
                                 {:atom2, 4, :atom2_1}]),
               types: [id: :atom, value: :odd, id_sub: :atom]}
              } ==
      R.t(fn() -> 
        L.extend(relvar, :id_sub, :atom, fn(t) -> 
          x = elem(t,0)
          :"#{x}_1" 
        end)
      end)
  end
  test "type selector" do
    create_type()
    assert {:atomic, true } ==
            R.t(fn() -> R.sel!(:atom, "a") === :a end)
    assert {:atomic, true } ==
            R.t(fn() -> R.sel!(:atom, "1") === :"1" end)
    assert {:atomic, true } ==
            R.t(fn() -> R.sel!(:odd, 2) === 2 end)
    assert {:aborted, 
            {%ArgumentError{message: "invalid type: 3 is not odd"},_}}
    = R.t(fn() -> R.sel!(:odd, 3) === 1 end)
    assert {:aborted, 
            {:badarith, _}}
    = R.t(fn() -> R.sel!(:odd, "a") === 1 end)

#    assert {:atomic, true } ==
#            R.t(fn() -> R.sel!(:odd, "a") === 2 end)

  end
  test "update" do
    create_type()
    relvar = R.to_relvar(:test2)
    ret = %L{body: MapSet.new([four: 4]),
            types: [id: :atom, value: :odd]}
    assert {:atomic, ret} == 
      R.t(fn() -> 
        L.where(relvar, &(&1[:value] == 4)) |> R.update(
                 fn(old) ->
                   IO.inspect [old: old]
                   m = put_in old, [:id], :four
                   IO.inspect [new: m]
                   m
                 end, relvar)
        L.where(relvar,&(&1[:value]==4))
      end)
  end
  test "delete" do
    create_type()
    relvar = R.to_relvar(:test2)
    ret = %L{body: MapSet.new(),
            types: [id: :atom, value: :odd]}
    assert {:atomic, ret} == 
      R.t(fn() -> 
        L.where(relvar, &(&1[:value] == 4)) |> R.delete(relvar)
        L.where(relvar,&(&1[:value]==4))
      end)
  end
  test "Reltuple" do
    m = %Reltuple{tuple_index: %{atom: 0, value: 1}, tuple: {:a, 1}}
    assert :a == m[:atom]
  end
  test "njoin/2" do
    create_type()
    m = R.create(:test3, [:value], [value: :odd, mark: :atom])
    R.t(fn() ->
      R.write(:test3, {4, :four})
      R.write(:test3, {2, :two})
    end)
    relvar = R.to_relvar(:test2)
    a = R.t(fn() -> 
      L.njoin(relvar, m)
    end)
    assert a == {:atomic, [{:atom2, 4, 4, :four},
                           {:atom1, 2, 2, :two}]}
#    IO.inspect [a: a]
    assert [:a, :b, :e] == 
      L.delete_elements_from_tuple(0,[2,3], 
                                   {:a, :b, :c, :d, :e})
  end
  @tag :test4
  test "relval.table/1" do
    create_type()
    relval = %L{body: MapSet.new([{:a, 4}, {:b, 6}, {:c, 8}]),
                types: [id: :atom, value: :odd]}
    qlc = Qlc.q("""
    [ {K, V} || {K,V} <- Y, K =:= a ]
    """, [Y: L.table(relval)])
    r = Qlc.e(qlc)
    #IO.puts :qlc.info(qlc)
    assert [r: r] == [r: [a: 4]]
  end
end
