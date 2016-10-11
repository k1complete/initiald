defmodule Constraint do
  require Record
  alias Relvar2, as: R
  alias Relval2, as: L
  require Qlc

  @relvar_constraint :__relvar_constraint__
  @constraint :__constraint__
  @constraint_fields [name: nil, type: :before, definition: nil]
  Record.defrecord @constraint, @constraint_fields
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
  def destroy() do
    R.drop(@relvar_constraint)
    R.drop(@constraint)
    Reltype.destroy()
  end
  @doc """
  create new constraint for relvars, by definition.
  """
  @spec create(String.t, list, function) :: true | no_return
  def create(constraint, relvars, definition) do
    c = R.to_relvar(@constraint)
    rc = R.to_relvar(@relvar_constraint)
    :ok = R.write(c, {constraint, definition})
    Enum.all?(relvars,
      fn(x) -> 
        :ok = R.write(rc, {x, constraint})
      end)
  end
  @spec validate([atom]) :: [] | [{false, map, tuple}]
  @doc """
  validate fro relname list.

  find related constraint from tables, and
  eval definition.
  return failed constraint list.

  """
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
  @spec foreign_key!(atom, atom, list) :: true | no_return
  def foreign_key!(pk, fk, keys) do
    pkvar = R.to_relvar(pk)
    fkvar = R.to_relvar(fk)
    s = L.minus(_fk=L.project(fkvar, keys, true),
                _pk=L.project(pkvar, keys, true))
    case Enum.count(s) == 0 do
      true -> 
        true
      false ->
        {:foreign_key, fkvar.name, pkvar.name, s}
    end
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

    しかし、実際は、変更したい{k, v}が定まっていると思われるので、
    その{k, v}についてのみ考えればいいはず。
    s[sno]を削除すると、影響するsp[sno]があるかもしれず、其の場合、
    エラーをかえしたいが、面倒臭い。
    
    [relvars: [:sp, :s], 
     definition: fn() -> forien_key(:s, :sp, :sno) end]
     def forien_key(pk, fk, keys) do 
        count(fnjoin(project(pk, keys), project(fk, keys))) ==
        count(project(fk, keys))
     end
    複数の代入が出来ればいいが、実用的じゃないかも。
    。

  """
end
