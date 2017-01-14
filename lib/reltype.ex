defmodule Reltype do
  require Record
  require Qlc
  @reltype :reltype
  @type typename :: atom
  @reltype_fields [typename: nil, definition: &(__MODULE__.truely/1), cast: nil]
  Record.defrecord @reltype, @reltype_fields
  def truely(_), do: true 

  def init() do
    :mnesia.create_table(@reltype, 
                         [type: :set,
                          attributes: Keyword.keys(@reltype_fields)])
  end
  @spec create(record(:reltype)) :: :ok | any
  def create(t) when Record.is_record(t, @reltype) do
    :mnesia.write(t)
  end
  def create(typename, definition, cast \\ nil) do
    create(reltype(typename: typename, definition: definition, cast: cast))
  end
  @spec validate(typename, any) :: true | false
  def validate(typename, value) when is_atom(typename) do
    qc = Qlc.q("""
    [true || {_, N, Definition,_} <- Q, 
               N =:= TypeName, 
               apply(Definition, [Value]) =:= true]
    """, [Q: table(), TypeName: typename, Value: value])
    r = Qlc.e(qc) |> Enum.count 
    r == 1
  end
  def read(type) do
    :mnesia.read(@reltype, type)
  end
  def destroy() do
    :mnesia.delete_table(@reltype)
  end
  @type erlang_error :: any()
  @spec delete(atom) :: :ok | erlang_error
  def delete(typename) when is_atom(typename) do
    :mnesia.delete({@reltype, typename})
  end
  @doc """
  get tableinfo for QLC
  """
  @spec table() :: :qlc.query_handle()
  def table() do
    :mnesia.table(@reltype)
  end
end
defmodule RelType.TypeConstraintError do
  defexception [type: nil, value: nil, attribute: nil]
  def message(exception) do
    "type #{inspect(exception.type)}, value: #{inspect(exception.value)}, attribute: #{inspect(exception.attribute)}"
  end
end
