defmodule InitialD.Builtin do
  require Record
  Record.defrecord :utc_timestamp, second: nil, microsecond: nil
  Record.defrecord :range, lower: nil, upper: nil, edge: "[)"
  
end
