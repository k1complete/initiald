defmodule RelationalTest do
  use ExUnit.Case, async: false
  require Qlc
  alias Relvar2, as: R
  alias Relval2, as: L
  require Reltype
  require Relvar2
  require Relval2
  @moduletag :test_rel

  setup_all do
    :mnesia.start
    Reltype.init
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
      IO.inspect [:mnesia.all_keys(:test2)]
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
    relval2 = %L{body: MapSet.new([{:atom1, 2}, {:atom3, 6}]),
                 types: [id: :atom, value: :odd]}
    assert R.t(fn() ->
      L.union(relvar, relval2)
      end) == {:atomic,%L{body: MapSet.new([{:atom1, 2},
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
    relval2 = %L{types: [id: :atom, value: :odd],
                 body: MapSet.new([{:atom1, 2}])}
    assert R.t(fn() ->
      L.intersect(relvar, relval2)
      end) == {:atomic, %L{body: MapSet.new([atom1: 2]),
                           types: [id: :atom, value: :odd]}}
  end
  test "Relational operator where" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert R.t(fn() ->
      L.where(relvar, fn(v) -> 
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
      L.project(relvar, [:value])
    end) == {:atomic, %L{body: MapSet.new([{2},{4}]),
                        types: [value: :odd]}}
  end
  test "Relational operator fnjoin" do
    create_type()
    relvar = R.to_relvar(:test2)
    relval2 = %L{body: MapSet.new([{2, :two}, {4, :four}, {6, :six}]),
                 types: [value: :odd, name: :atom]}

    assert R.t(fn() ->
      L.fnjoin(relvar,  relval2)
    end) == {:atomic,%L{body: MapSet.new([{:atom1, 2, :two},
                                          {:atom2, 4, :four}]),
                        types: [id: :atom, value: :odd, name: :atom]}}
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
#    relvar = R.to_relvar(:test2)
#    assert {:atomic, 
#            %{%{id: :atom1} => %{value: 2, id_sub: :atom1_1}, 
#              %{id: :atom2} => %{value: 4, id_sub: :atom2_1}}} ==
#      R.t(fn() -> 
#        Initiald.extend_add(relvar, :id_sub, fn({k,_v}) -> 
#          x = Map.get(k, :id)
#          :"#{x}_1" 
#        end)
#      end)
  end
end
