alias InitialD.Constraint
alias InitialD.Relval
alias InitialD.Relvar
alias InitialD.Reltype
alias InitialD.Relutil
alias InitialD.Reltuple

defmodule InitialD.View do
  require Logger
  require Qlc
#  alias Relval2, as: L
  require Reltuple
  alias Reltuple, as: T
  alias Relvar, as: R
  alias Relval, as: L
  @behaviour Access

  @key :_key
  @view :_view
#  @relname :_relname
  def init() do
    R.t(fn() ->
      Reltype.create(:function, &is_function/1)
      Reltype.create(:binary, &is_binary/1)
    end)
    r = R.create(@view, [:name], [name: :atom, definition: :function, source: :binary])
#    IO.inspect [view: r]
  end
  def create(name, ast) do
    {exp, []} = Code.eval_quoted(ast)
    case Relvar.write(@view, {name, exp, Macro.to_string(ast)}) do
      :ok ->
        r = to_relvar(name)
#        IO.inspect(r)
        r
      x -> 
        x
    end
  end
  def to_relvar(name) when is_atom(name) do
    case :mnesia.read({@view, name}) do
      [] -> 
        :mnesia.abort({:"not_found_view", name})
      [{@view, ^name, ^name, exp, source}] ->
        IO.puts source
        exp.()
    end
  end
  def drop(name) do
    :mnesia.delete({@view, name})
  end
  
end
