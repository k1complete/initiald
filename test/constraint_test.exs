defmodule ConstraintTest do
  use ExUnit.Case, async: false
  require Qlc
  use InitialD
  alias Relvar, as: R
  alias Relval, as: L
  require Relvar
  require Relval
  require Reltype
  require Constraint
  @moduletag :test3
#  @key :_key

  setup_all do
    :mnesia.start
    Reltype.init()
    Constraint.init()
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
        IO.inspect [odd: x]
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
  end
  test "create/3" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_22_fk") end)
    end
    R.t(fn() ->
      Constraint.create("test_2_22_fk", [:test22, :test2], fn(_x) ->
        Constraint.foreign_key!(:test2, :test22, [:value])
      end)
    end)
    assert({:atomic, :ok} == R.t(fn() -> 
      R.write(:test2, {:id1, 4})
    end))
    assert({:atomic, :ok} == R.t(fn() -> 
      R.write(:test22, {4, :m})
      Constraint.validate([:test22])
    end))
    assert({:aborted, [{false, "test_2_22_fk", 
                        {:foreign_key, :test22, :test2, _r }}]} = 
      R.t(fn() -> 
        R.write(:test22, {6, :m})
#        relvar = R.to_relvar(:test22)
        Constraint.validate([:test22])
    end))
    assert({:aborted, [{false, "test_2_22_fk",
                        {:foreign_key, :test22, :test2, _r}}]} = 
      R.t(fn() -> 
#        test2 = R.to_relvar(:test2)
#        test22 = R.to_relvar(:test22)
        R.delete(R.to_relvar(:test2), {:id1, 4})
        Constraint.validate([:test2])
      end))
  end
  @tag :empty?
  test "empty?" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_22_check") end)
    end
    s = R.t(fn() ->
      Constraint.create("test_2_22_check", [:test2], fn(_x) ->
        Constraint.empty?(fn() ->
          R.to_relvar(:test2) 
          |> L.where([], (value != 6))
        end)
      end)
    end)
    assert {:atomic, true} == s
    t = R.t(fn() -> 
      R.write(:test2, {:id2, 6})
      Constraint.validate([:test2])
      L.project(R.to_relvar(:test2), [:id, :value]) |> L.execute()
    end)
    assert R.t(fn() ->
               L.new(%{body: [{:id2, 6}],
                       keys: [:id, :value],
                       name: :test2,
                       types: [id: :atom, value: :odd]}) |> L.execute()
             end)
           == t
    t = R.t(fn() ->
      R.to_relvar(:test2) 
      |> R.write({:id2, 4})
    end)
    assert {:aborted, [{false, "test_2_22_check", false}]} == t
#    IO.inspect [t: t]
    u = R.t(fn() -> 
      R.write(:test2, {:id3, 4})
      Constraint.validate([:test2])
    end)
    assert {:aborted, [{false, "test_2_22_check", false}]} == u
  end
  @tag :exclude
  test "generic_exclude?" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_exclude_generic") end)
    end
    R.t(fn() ->
      Constraint.create("test_2_exclude_generic", [:test2], fn(_x) ->
        Constraint.generic_exclude?(:test2, fn(t1, t2) ->
          IO.inspect [t1: t1.tuple, t2: t2.tuple]
          t1[:value] != t2[:value]
          end)
      end)
    end)
    assert {:aborted, 
       [{false, "test_2_exclude_generic", 
         {:exclude,
          %InitialD.Reltuple{},
          %InitialD.Reltuple{}}}]} = 
      R.t(fn() ->
        :mnesia.write({:test2, {:id2, 6}, :id2, 6} )
        :mnesia.write({:test2, {:id2, 4}, :id2, 4} )
        :mnesia.write({:test2, {:id2, 2}, :id2, 2} )
        :mnesia.write({:test2, {:id2, 8}, :id2, 8} )
        :mnesia.write({:test2, {:id3, 8}, :id3, 8} )
        IO.inspect [write: :write]
        Constraint.validate([:test2])
      end)

  end
  @tag :exclude_abort
  test "exclude_abort" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_exclude") end)
    end
    assert {:aborted, {%InitialD.ConstraintRequiredIndexError{}, st}} = R.t(fn() ->
      Constraint.create("test_2_exclude", [:test2], 
                        fn(_x) ->
                          Constraint.exclude?(:test2, :value, fn(t1, t2) ->
                            t1 != t2
                          end)
                        end, 
                        fn() ->
                          Constraint.require_index(:test2, :value)
                        end)
    end)
    {:atomic, :ok} = :mnesia.add_table_index(:test2, :value)
    assert {:atomic, true}  = R.t(fn() ->
      Constraint.create("test_2_exclude", [:test2], 
                        fn(_x) ->
                          Constraint.exclude?(:test2, :value)
                        end, 
                        fn() -> 
                          Constraint.require_index(:test2, :value) 
                        end)

    end)
  end
  test "exclude?" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_exclude") end)
    end
    {:atomic, :ok} = :mnesia.add_table_index(:test2, :value)
    assert {:atomic, true}  = R.t(fn() ->
      Constraint.create("test_2_exclude", [:test2], 
                        fn(_x) ->
                          Constraint.exclude?(:test2, :value)
                        end, 
                        fn() -> 
                          Constraint.require_index(:test2, :value) 
                        end)

    end)
    assert {:atomic, :ok} =
      R.t(fn() ->
        :mnesia.write({:test2, {:id2, 6}, :id2, 6} )
        :mnesia.write({:test2, {:id2, 4}, :id2, 4} )
        :mnesia.write({:test2, {:id2, 2}, :id2, 2} )
        :mnesia.write({:test2, {:id2, 8}, :id2, 8} )
        :mnesia.write({:test2, {:id3, 8}, :id3, 8} )
        IO.inspect [write: :write]
        Constraint.validate([:test2])
      end)
  end
  @tag :unique?
  test "unique?" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_unique") end)
    end
    assert {:atomic, true} = R.t(fn() ->
      Constraint.create("test_2_unique", [:test2],
        fn(_t) ->
          Constraint.unique?(:test2, [:value], fn(x) -> 
            L.where(x, (value < 6))
          end)
        end)
    end)
    assert {:aborted,
            [{false, "test_2_unique",
              {:unique?, [{:test2, 2, 2, 2}], [value: :odd, c: :int]}}]}
    = R.t(fn() ->
      :mnesia.write({:test2, {:id2, 6}, :id2, 6} )
      :mnesia.write({:test2, {:id2, 4}, :id2, 4} )
      :mnesia.write({:test2, {:id2, 2}, :id2, 2} )
      :mnesia.write({:test2, {:id2, 8}, :id2, 8} )
      :mnesia.write({:test2, {:id3, 2}, :id3, 2} )
      IO.inspect [write: :write]
      Constraint.validate([:test2])
    end)
  end
  
  @tag :range_exclude
  test "range_exclude" do
    
  end
end
