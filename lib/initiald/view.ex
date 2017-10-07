#alias InitialD.Constraint
alias InitialD.Relval
alias InitialD.Relvar
alias InitialD.Reltype
#alias InitialD.Relutil
alias InitialD.Reltuple

defmodule InitialD.View do
  require Logger
  require Qlc
#  alias Relval2, as: L
  require Reltuple
  require Relval
#  alias Reltuple, as: T
  alias Relvar, as: R
#  alias Relval, as: L
  @behaviour Access

#  @key :_key
  @view :_view
#  @relname :_relname
  def init() do
    R.t(fn() ->
      Reltype.create(:function, &is_function/1)
      Reltype.create(:binary, &is_binary/1)
    end)
    r = R.create(@view, [:name], [name: :atom, definition: :function, source: :binary])
#    IO.inspect [view: r]
    r
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
      [{@view, ^name, ^name, exp, _source}] ->
#        IO.puts source
        exp.()
    end
  end
  def drop(name) do
    :mnesia.delete({@view, name})
  end
  def get(t, key, default \\ nil) do
    case fetch(t, key) do
      :error -> default
      {:ok, value} -> value
    end
  end
  def get_and_update(t, key, f) do
    case f.(get(t, key)) do
      :pop ->
        pop(t, key)
      {old, new} ->
        {old, new}
    end
  end
  def pop(t, key) when is_tuple(key) do
    sels = Tuple.to_list(key)
    atts = Keyword.keys(t.types)
    sels = atts -- sels
    case sels do
      [] -> :error
      _x -> {:ok, Relval.project(t, sels)}
    end
  end
  def fetch(t, key) when is_tuple(key) do
    sels = Tuple.to_list(key)
    atts = Keyword.keys(t.types)
    case sels -- atts do
      [] -> {:ok, Relval.project(t, sels)}
      _x -> :error
    end
  end
  
end
