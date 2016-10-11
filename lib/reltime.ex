defmodule RelTime do
  @doc """
  erlang timestamp type {megasec, sec, nanosec}
  
  """
  @type megasec :: integer
  @type sec :: 0..999999
  @type nanosec :: 0..999999999
  @type year :: 1970..10000
  @type month :: 1..12
  @type day :: 1..31
  @type hour :: 0..23
  @type minute :: 0..59
  @type second :: 0..59
  def now() do
    :erlang.timestamp()
  end
  @spec to_local_time({megasec, sec, nanosec}) :: {{year, month, day},{hour, minute, second}, {nanosec}}
  def to_local_time({_megasec, _sec, nanosec} = t) do
    {d, h} = :calendar.now_to_local_time(t)
    {d, h, nanosec}
  end
end

  
