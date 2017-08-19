alias InitialD.Relval
alias InitialD.Relvar
alias InitialD.Reltype
alias InitialD.Reltuple
defmodule InitialD.Constraint do
  require Record
  alias Relvar, as: R
  alias Relval, as: L
  require Qlc
  @relvar_constraint :__relvar_constraint__
  @constraint :__constraint__
  @constraint_fields [name: nil, type: :before, definition: nil]
  Record.defrecord @constraint, @constraint_fields
  @type t :: record(:__constraint__, name: atom, type: atom, definition: fun)
  @doc """
  initialize constraint type and relvar.
  relvar table :: @constraint 
                  @constraint_fields 
  """
  @spec init() :: :ok | no_return
  def init() do
    {:atomic, :ok} = R.t(fn() ->
      Reltype.create(:string, fn(x) -> String.valid?(x) end)
      Reltype.create(:function, &(is_function(&1)))
      Reltype.create(:atom, &(is_atom(&1)))
    end)
    c = R.create(@constraint, [:constraint], 
                 [constraint: :string, definition: :function])
    rc = R.create(@relvar_constraint, [:relvar, :constraint], 
                  [relvar: :atom, constraint: :string])
    {c, rc}
    :ok
  end
  @spec destroy() :: :ok | no_return
  def destroy() do
    R.drop(@relvar_constraint)
    R.drop(@constraint)
    Reltype.destroy()
  end
  @doc """
  create new constraint for relvars, by definition.
  """
  @spec create(String.t, [atom()], (any -> boolean | no_return)) :: boolean() | no_return()
  def create(constraint, relvars, definition, contract \\ fn() -> {:ok, true} end) do
    case contract.() do
      {:ok, _} ->
        c = R.to_relvar(@constraint)
        rc = R.to_relvar(@relvar_constraint)
        R.write(c, {constraint, definition})
        Enum.all?(relvars, fn(x) -> :ok == R.write(rc, {x, constraint}) end)
      {:error, e} ->
        {false, e}
    end
  end
  def require_index(table, att) do
    i = Enum.find_index(:mnesia.table_info(table, :attributes),
                        &(att == &1)) + 2
    case :mnesia.table_info(table, :index) do
      [^i] -> 
        IO.inspect [index: att, i: i]
        {:ok, i}
      x ->
        IO.inspect [index_ng: x, i: i]
        raise(InitialD.ConstraintRequiredIndexError, [relname: table, 
                                                      attribute: att])
    end
  end
  @spec delete(String.t) :: :ok | no_return
  def delete(constraint) do
    qc = Qlc.q("""
         [ {RV, RC} || {_, {RV, RC}, RV, RC} <- RelvarConstraint,
         RC =:= C ]
         """, [RelvarConstraint: R.table(@relvar_constraint), C: constraint])
    case Qlc.e(qc) do
      [] ->
        :ok
      x when is_list(x) ->
        Enum.each(x, fn(x) -> 
          :mnesia.delete(@relvar_constraint, x, :write) 
        end)
    end
    :mnesia.delete(@constraint, constraint, :write)
  end
  @doc """
  validate fro relname list.

  find related constraint from tables, and
  eval definition.
  return failed constraint list.

  """
  @spec validate([atom]|R.t) :: :ok | no_return
  def validate(%R{} = relname) do
    validate([relname.name])
  end
  def validate(relnames) when is_list(relnames) do
    qc = Qlc.q("""
      [ {false, RC, Ret} || {_, C, C, D} <- Constraint,
                {_, {RV, RC}, RV, RC} <- RelvarConstraint,
                RN <- Relnames,
                RC =:= C,
                RN =:= RV,
                (Ret = D(RN)) =/= true
                ]
    """, [Constraint: R.table(@constraint),
          RelvarConstraint: R.table(@relvar_constraint),
          Relnames: relnames])
    case Qlc.e(qc) do
      [] ->
#        IO.puts :qlc.info(qc)
#        IO.inspect [C: qc]
        :ok
      x ->
        :mnesia.abort(x)
    end
  end
  @doc """
  builtin constraint: typical foreign_key constraint.

  pk : primary key side relvar name(atom)
  fk : foreign key side relvar name(atom)
  keys: keyname list(atom list)

  if not satisfy constraint, raise transaction abort.
  """
  def foreign_key!(pk, fk, keys) do
    pkvar = R.to_relvar(pk)
    fkvar = R.to_relvar(fk)
    s = L.minus(L.do_project(fkvar, keys),
                L.do_project(pkvar, keys)) 
    case empty?(fn() -> s end) do
      true -> 
        true
      false ->
        {:foreign_key, fkvar.name, pkvar.name, s}
    end
  end
  @doc """
  empty?((() -> Relval)) :: integer
  """
  def empty?(r) do
    case Enum.count(r.()|>L.execute()) == 0 do
      true -> true
      false ->
        false
    end
  end
  defp loop_b(table, types, a, at, b, f) do
    case b do
      :"$end_of_table" ->
#        IO.inspect [a: a, b: b]
        true
      b ->
        [br] = :mnesia.read(table, b)
        bt = Reltuple.raw_new(br, types)
#        IO.inspect [a: a, b: b]
        case f.(at, bt) do
          true -> 
            b = :mnesia.next(table, b)
            loop_b(table, types, a, at, b, f)
          false ->
            {:exclude, at, bt}
        end
    end
  end
  defp loop_a(table, types, a, f) do
    case a do
      :"$end_of_table" ->
#        IO.inspect [a: a]
        true
      a ->
        [ar] = :mnesia.read(table, a)
        at = Reltuple.raw_new(ar, types)
        b = :mnesia.next(table, a)
        r = case b do
              :"$end_of_table" ->
                true
              b ->
                loop_b(table, types, a, at, b, f)
            end
        case r do
          true ->
            loop_a(table, types, :mnesia.next(table, a), f)
          _ ->
           r 
        end
    end
  end
  @doc """
  関係変数中の全タプルがop/2を適用すると全てfalseとなる
  例: relvar r中の属性aがuniqという関係は
      exclude?(r, fn(t1, t2) -> t1[:a] != t2[:a] end)
  """
  def generic_exclude?(r, f) do
    relvar = R.to_relvar(r)
    table = r
    IO.inspect [keys: :mnesia.all_keys(table)]
    IO.inspect [exclude: :exlucede]
    types = relvar.types
    loop_a(table, types, :mnesia.first(table), f)
  end
  defp exloop_b(index_table, index, aterm, :"$end_of_table", op) do
    true
  end
  defp exloop_b(itable, index, {av, ak}, b = {bv, bk}, op) do
    case op.(av, bv) do
      true ->
        exloop_b(itable, index, {av, ak}, :ets.next(itable, b), op)
      ret ->
        ret
    end
  end
  defp exloop(itable, index, :"$end_of_table", op) do
    true
  end
  defp exloop(itable, index, a, op ) do
    case exloop_b(itable, index, a, an = :ets.next(itable, a), op) do
      true ->
        exloop(itable, index, an, op)
      ret ->
        ret
    end
  end
  def exclude?(table, elem_name, op \\ fn(x, y) -> x != y end) do
    m = :mnesia.table_info(table, :index_info)
    IO.inspect [m: m]
    {:index, :set, [{{i, :ordered}, 
                    {storage, index_table}}]} = :mnesia.table_info(table, :index_info)
    exloop(table, i, :ets.first(index_table), op)
  end
  @moduledoc """
  constraint.

  特定の関係変数を更新する前に満しているべき性質。
  条件を満さない場合は、transaction abortする。
  
    複数の関係変数間で使える。
    つまり、write時ではなく、writeが終ったあとに纏めて制約整合性を
    確認することになる。

    例: forieng-key
    1. sp[sno]は必ずs[sno]がないといけない。
    2. sp[pno]は必ずp[pno]がないといけない。

    事後制約的な命題としての記述
    definition1 = fn() ->
      Enum.count(L.fnjoin(L.project(sp, :sno), s)) ==
        Enum.count(L.project(s, :sno))
    end
    definition2 = fn() ->
      Enum.count(L.project(sp, :pno)) <= Enum.count(L.project(p, :pno))
    end

    しかし、実際は、変更したいタプルが定まっていると思われるので、
    そのタプルについてのみ考えればいいはず。
    s[sno]を削除すると、影響するsp[sno]があるかもしれず、其の場合、
    エラーをかえしたいが、面倒臭い。
    
    [relvars: [:sp, :s], 
     definition: fn() -> forien_key(:s, :sp, :sno) end]
     def forien_key(pk, fk, keys) do 
        count(fnjoin(project(pk, keys), project(fk, keys))) ==
        count(project(fk, keys))
     end
    複数の代入が出来ればいいが、実用的じゃないかも。

  """
end
defmodule InitialD.ConstraintRequiredIndexError do
  defexception [relname: nil, attribute: nil]
  def message(exception) do
    "relname #{inspect(exception.relname)}, attribute: #{inspect(exception.attribute)}"
  end
end
