ExUnit.start()
defmodule Relational_Test4 do
  use ExUnit.Case, async: false
  require Qlc
  use InitialD
  require Relvar
  require Relval
  require Relval.Assign
  require Reltype
  alias Relvar, as: R
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
#    keys = [:id, :id2]
#    types = [id: :atom, value: :odd, id2: :atom]
    on_exit fn ->
      R.drop(:test2)
    end
    R.create(:test2, [:id, :id2], [id: :atom, value: :odd, id2: :atom])
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
  @tag :assign
  test "assign" do
    create_type()
    relvar = R.to_relvar(:test2)
    assert {:atomic, 
            [{:test2, {:a3, :a23}, :a3, 12, :a23},
             {:test2, {:a4, :a24}, :a4, 14, :a24},
             {:test2, {:atom2, :a2}, :atom2, 4, :a2}]} == 
      R.t(fn() ->
        L.assign [s: s] do
#        update: L.where(relvar, (value == 2)) ->
#          [id2: old[:id], value: s+1]
        insert: relvar ->
          L.new(%{types: [value: :odd, id: :atom, id2: :atom],
              body: [{12, :a3, :a23},
                     {14, :a4, :a24}],
              keys: [:id, :id2],
              name: :test2})
        delete: L.where(relvar, (value == 2)) ->
          old
#       true
      end
      relvar |> L.execute() |> Enum.sort()
    end)
  end
  test "assign2" do
    create_type()
    relvar = R.to_relvar(:test2)
    s = 1
    assert {:atomic, _ret} = R.t(fn() ->
      L.assign [s: s] do
        update: L.where(relvar, (value == 2)) ->
#          IO.inspect [s: s]
          [id2: old[:id], value: s+1]
#        insert: relvar ->
#          L.new(%{types: [value: :odd, id: :atom, id2: :atom],
#              body: [{12, :a4, :a23}],
#              keys: [:id, :id2],
#              name: :test2})
#        delete: L.where(relvar, (value == 2)) ->
#          old
#       true
      end
    end)
#    IO.inspect [ret: ret]
  end
  @tag :assign2
  test "type3" do
    assert catch_error(raise(Reltype.TypeConstraintError, 
                             [type: :odd, 
                              value: 3,
                              attribute: :value])) == 
      %Reltype.TypeConstraintError{attribute: :value, type: :odd, value: 3}
  end
  test "assign3" do
    create_type()
    relvar = R.to_relvar(:test2)
    s = 1
    assert {:aborted, 
            {:error, 
             %Reltype.TypeConstraintError{attribute: :value, type: :odd,
                                          value: 3}}} ==
      R.t(fn() ->
        L.assign [s: s] do
          update: L.where(relvar, (value == 2)) ->
            [id2: old[:id], value: old[:value]+1]
          #        insert: relvar ->
          #          L.new(%{types: [value: :odd, id: :atom, id2: :atom],
          #              body: [{12, :a4, :a23}],
          #              keys: [:id, :id2],
          #              name: :test2})
          #        delete: L.where(relvar, (value == 2)) ->
          #          old
          #       true
        end
      end)
  end
  @tag :assign2
  test "assign4" do
    create_type()
    relvar = R.to_relvar(:test2)
    s = 1
    assert {:aborted, 
            {:error, 
             %Relval.ConstraintError{
               constraints: [
                 %Reltype.TypeConstraintError{attribute: :value, type: :odd,
                                              value: 11},
                 %Reltype.TypeConstraintError{attribute: :id, type: :atom,
                                              value: 3}],
                 relname: :test2}}} ==
      R.t(fn() ->
        L.assign [s: s] do
          update: L.where(relvar, (value == 2)) ->
            [id2: old[:id], value: old[:value]+2]
          insert: relvar ->
            L.new(%{types: [value: :odd, id: :atom, id2: :atom],
                    body: [{11, 3, :a23}],
                    keys: [:id, :id2],
                    name: :test2})
          delete: L.where(relvar, (value == 2)) ->
            old
            #       true
        end
      end)
  end
end
