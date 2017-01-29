defmodule TeddTest do
  use ExUnit.Case, async: false
  require Logger
  require Qlc
  use InitialD
  require Constraint
  alias Relvar2, as: R
  alias Relval, as: L
  require Relval
  require Reltype
  @moduletag :test2

  setup_all do
    :mnesia.start
    Reltype.init
    Constraint.init
    setup()
    on_exit fn() ->
      Constraint.destroy
    end
    :ok
  end
  def setup() do
    m = Reltype.reltype(typename: :sno, 
                        definition: fn(x) -> is_binary(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :sname, 
                        definition: fn(x) -> is_binary(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :status, 
                        definition: fn(x) -> is_integer(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :city, 
                        definition: fn(x) -> is_binary(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :pno, 
                        definition: fn(x) -> is_binary(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :pname, 
                        definition: fn(x) -> is_binary(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :color, 
                        definition: fn(x) -> is_atom(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :weight, 
                        definition: fn(x) -> is_float(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    m = Reltype.reltype(typename: :qty, 
                        definition: fn(x) -> is_integer(x) end)
    assert({:atomic, :ok} == R.t(fn() -> Reltype.create(m) end))
    s = R.create(:s, [:sno], [sno: :sno, 
                               sname: :sname, 
                               status: :status, 
                               city: :city])
    p = R.create(:p, [:pno], [pno: :pno,
                               pname: :pname,
                               color: :color,
                               weight: :weight,
                               city: :city])
    sp = R.create(:sp, [:sno, :pno], [sno: :sno, 
                                      pno: :pno, 
                                      qty: :qty])
    R.t(fn() ->
      Enum.into([{"s1", "smith", 20, "london"},
                 {"s2", "jones", 10, "paris"},
                 {"s3", "blake", 30, "paris"},
                 {"s4",  "clark", 20, "london"},
                 {"s5",  "adams", 30, "athens"}],
                s)
      Enum.into([{"p1", "nut", :red,  12.0,  "london"},
                 {"p2", "bolt", :green,  17.0, "paris"},
                 { "p3", "screw", :blue,  17.0,  "oslo"},
                 { "p4", "screw", :red,  14.0,  "london"},
                 { "p5", "cam", :blue,  12.0,  "paris"},
                 { "p6", "cog", :red,  19.0,  "london"}],
              p)
      Enum.into([{"s1",  "p1",  300},
                 { "s1",  "p2",  200},
                 { "s1",  "p3",  400},
                 { "s1",  "p4",  200},
                 { "s1",  "p5",  100},
                 { "s1",  "p6",  100},
                 { "s2",  "p1",  300},
                 { "s2",  "p2",  400},
                 { "s3",  "p2",  200},
                 { "s4",  "p2",  200},
                 { "s4",  "p4",  300},
                 { "s4",  "p5",  400}],
                sp)
    end)
  end
  setup do
    {:ok, %{s: R.to_relvar(:s), 
            p: R.to_relvar(:p), 
            sp: R.to_relvar(:sp)}}
  end
  test "relational compare", c do
    s = Map.get(c,:s)
#    p = Map.get(c,:p)
#    sp = Map.get(c, :sp)
    on_exit fn ->
#      IO.puts "destroy"
      R.t(fn ->
        :ok
      end)
    end
    R.t(fn() ->
      R.write(s, %{:sno=>"s2", :sname => "Jones", :status => 30, :city => "paris"})
      sr = s
      assert Enum.count(L.project(sr, [:city])) == 
        Enum.count(L.project(sr, [:city, :status]))
    end)
  end
  @tag :cc
  test "summarize", c do
    s = Map.get(c,:s)
    _p = Map.get(c,:p)
    sp = Map.get(c, :sp)
 #   IO.inspect [sp: sp, s: s]

    r =R.t(fn() ->
      L.summarize(sp, 
                  L.where(L.project(s, [:sno]), 
                  (sno == "s1" or sno == "s2" or sno == "s3" or sno == "s5" )
                  ),
                  add: {fn(t) ->
#                         IO.puts 'QLC: ' ++  :qlc.info(t.query)
#                         IO.inspect [Tuple: t, 
#                                     List: L.execute(t),
#                                     QLC: ""]
                         {L.count(t),
                          L.max(t, :qty)}
                       end, 
                        [pccount: :int, 
                         qmax: :int]
                       }
      ) |> L.execute() |> Enum.sort()
    end)
    assert r == {:atomic, L.raw_new(%{types: 
                                      [sno: :sno, pccount: :int, qmax: :int],
                                  keys: [:sno],
                                  name: :s,
                                  body: [{:s, "s1", "s1", 6, 400},
                                         {:s, "s2", "s2", 2, 400},
                                         {:s, "s3", "s3", 1, 200},
                                         {:s, "s5", "s5", 0, nil}]})
                 |> L.execute() |> Enum.sort()
                }
  end
  test "constraint create" do
    R.t(fn() ->
      Constraint.create("s_sp_fk", [:s, :sp], 
                      fn(_rel) -> 
#                        IO.inspect [foreign_key: R.to_relvar(:sp)]
                        Constraint.foreign_key!(:s, :sp, [:sno]) 
                      end)
      Constraint.create("p_sp_fk", [:p, :sp], 
                      fn(_rel) -> 
#                        IO.inspect [foreign_key: R.to_relvar(:sp)]
                        s = Constraint.foreign_key!(:p, :sp, [:pno]) 
#                        IO.inspect [foreign_key_s: s]
                        s
                      end)
    end)
    r = R.t(fn() ->
      Enum.into([{"s1", "p60", 1}], 
                R.to_relvar(:sp))
      case Constraint.validate([:sp]) do
        [] ->
          true
        reason ->
          :mnesia.abort(reason)
      end
    end)
    assert {:aborted, [{false, "p_sp_fk",
                        {:foreign_key, :sp, :p, _}}]} = r
#    IO.inspect [s1: r]
    r = R.t(fn() ->
      Enum.into([{"s1",  "p6",  1}], 
                R.to_relvar(:sp))
      case Constraint.validate([:sp]) do
        [] ->
          true
        :ok ->
          :ok
        reason ->
          :mnesia.abort(reason)
      end
    end)
    assert {:atomic, :ok} = r
    r = R.t(fn() ->
      R.delete(R.to_relvar(:s), {"s1"})
      case Constraint.validate([:s]) do
        [] ->
          true
        reason ->
          :mnesia.abort(reason)
      end
    end)
    assert ({:aborted, [{false,  "s_sp_fk", 
                        {:foreign_key, 
                         :sp, :s, _}}]} = r)
#    IO.inspect [s: r]

  end
  
end
