defmodule ConstraintTest do
  use ExUnit.Case, async: false
  require Qlc
  alias Relvar2, as: R
  alias Relval, as: L
  require Relvar2
  require Relval
  require Reltype
  require Constraint
  @moduletag :test3
  @key :_key

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
    assert(struct(R, %{:keys => keys, 
                       :name => :test2,
                       :types => types,
                       :attributes => [@key, :id, :value]
        })
           == R.create(:test2, [:id], [id: :atom, value: :odd]))
    m = R.create(:test22, [:value], [value: :odd, mark: :atom])
    assert(struct(R, %{:keys => [:value],
                       :name => :test22,
                       :types => [value: :odd, mark: :atom],
                       :attributes => [@key, :value, :mark]})
           == m)
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
  @tag :is_empty
  test "is_empty" do
    create_type()
    on_exit fn ->
      R.drop(:test2)
      R.drop(:test22)
      R.t(fn() -> Constraint.delete("test_2_22_check") end)
    end
    s = R.t(fn() ->
      Constraint.create("test_2_22_check", [:test2], fn(_x) ->
        Constraint.is_empty(fn() ->
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
    assert {:atomic, 
            L.new(%{body: [{:id2, 6}],
                    keys: [:id, :value],
                    name: :test2,
                    types: [id: :atom, value: :odd]}) |> L.execute()} == t
    t = R.t(fn() ->
      R.to_relvar(:test2) 
      |> R.write({:id2, 4})
    end)
    IO.inspect [t: t]
    u = R.t(fn() -> 
      R.write(:test2, {:id3, 4})
      Constraint.validate([:test2])
    end)
    assert {:aborted, [{false, "test_2_22_check", false}]} == u
  end
end
