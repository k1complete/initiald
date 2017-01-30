defmodule TutorialDTest do
  use ExUnit.Case
#  import InitialD
  use InitialD
  import InitialD
  require Reltype
  require Relval
  alias Relvar, as: R
  @moduletag :initiald
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
    assert({:atomic, :ok} == Relvar.t(fn() -> Reltype.create(m) end))
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

  doctest InitialD
  test "where" do
    assert R.t(fn() ->
      where(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
                         body: [{12, :a1, :a23},
                                {14, :a1, :a24},
                                {16, :a1, :a25}],
                         keys: [:id, :id2],
                         name: :test2}), [],
            value == 14) |>
        Relval.execute() |>
        Enum.sort()
    end) == 
      {:atomic,
       [{:test2, {:a1, :a24}, 14, :a1, :a24}]}
  end
  test "project" do
    assert R.t(fn() ->
      project(Relval.new(%{types: [value: :odd, id: :atom, id2: :atom],
                         body: [{12, :a1, :a23},
                                {14, :a1, :a24},
                                {16, :a1, :a25}],
                         keys: [:id, :id2],
                         name: :test2}), [:id, :value]) |>
        Relval.execute() |>
        Enum.sort()
    end) == 
      {:atomic, 
       [{:test2, {:a1, 12}, :a1, 12}, 
        {:test2, {:a1, 14}, :a1, 14}, 
        {:test2, {:a1, 16}, :a1, 16}]}
  end
end
