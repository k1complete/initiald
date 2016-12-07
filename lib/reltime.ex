defmodule RelTime do
  @doc """
  erlang timestamp type {megasec, sec, nanosec}
  
  """
  @type megasec :: non_neg_integer()
  @type sec :: non_neg_integer()
  @type nanosec :: non_neg_integer()
  @type year :: 1..1114111
  @type month :: 1..12
  @type day :: 1..255
  @type hour :: byte()
  @type minute :: byte()
  @type second :: byte()
  def now() do
    :erlang.timestamp()
  end
  @spec to_local_time({megasec, sec, nanosec}) :: {{year, month, day},{hour, minute, second}, nanosec}
  def to_local_time({_megasec, _sec, nanosec} = t) do
    {d, h} = :calendar.now_to_local_time(t)
    {d, h, nanosec}
  end
end

  
