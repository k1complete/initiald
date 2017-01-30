defmodule InitialD.Relutil do
  @doc """
  database record to pure tuple

  {table, key, att1, att2, ...} --> {att1, att2, ...}
  """
  @spec record_to_tuple(tuple) :: tuple
  def record_to_tuple(e) do
    t = Tuple.delete_at(e, 0)
    Tuple.delete_at(t, 0)
  end
end
