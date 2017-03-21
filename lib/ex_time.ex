defmodule ExTime.Util do
  defmacro __using__(_) do
  end
  defmacro is_year(year) do
    quote bind_quoted: [year: year] do
      is_integer(year)
    end
  end
  defmacro is_month(m) do
    quote do
      is_integer(unquote(m)) and unquote(m) >= 1 and unquote(m) <= 12
    end
  end
  defmacro is_day(d) do
    quote do
      is_integer(unquote(d)) and unquote(d) >= 1 and unquote(d) <= 31
    end
  end
end
defmodule ExTime do
  require Record
  use ExTime.Util
  import ExTime.Util

  Record.defrecord :date, [year: nil,
                           month: nil,
                           day: nil]
  Record.defrecord :time, [hour: nil,
                           minute: nil,
                           second: nil,
                           milisecond: nil]
  Record.defrecord :datetime, [date: nil, time: nil, zone: nil]
  Record.defrecord :interval, [century: 0,
                               decade: 0,
                               year: 0,
                               quarter: 0,
                               month: 0,
                               week: 0,
                               day: 0,
                               hour: 0,
                               minute: 0,
                               second: 0,
                               millisecond: 0]
  def day_of_month(month, leap) do
    leap = case leap do
             true -> 1
             false -> 0
           end
    [[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
     [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]]
    |> Enum.at(leap) |> Enum.at(month-1)
  end
  def is_leap_year(year) when is_integer(year) do
    rem(year, 4) == 0 and ((!(rem(year, 100) == 0)) or (rem(year, 400) == 0))
  end
  def adjust({:date, year, month, 0}) do
    IO.inspect [year: year, month: month]
    adjust({:date, year, month-1, day_of_month(month-1, is_leap_year(year))})
  end
  def adjust({:date, year, month, day}) do
    IO.inspect [year: year, month: month, day: day]
    year = year + div((month - 1), 12)
    leap = is_leap_year(year)
    month = rem(month - 1, 12) + 1
    {year, month} = case month <= 0 do
                      true -> {year - 1, 12 + month}
                      false -> {year, month}
                    end
    mday = day_of_month(month, leap)
    rday = rem(day - 1, mday) + 1
    dday = div(day - 1, mday)
    case dday > 0 do
      true -> 
        adjust({:date, year, month + dday, rday})
      false ->
        {:date, year, month, rday}
    end
  end
  def add({:date, year, month, day} = date, interval) when is_integer(year) and is_month(month) and is_day(day)  do
    year = year + interval(interval, :year) + 
      interval(interval, :century) * 100 + 
      interval(interval, :decade) * 10
    month = month + interval(interval, :month) + interval(interval, :quarter) * 3
    {:date, year, month, 1} = adjust({:date, year, month, 1})
    days = day_of_month(month,  is_leap_year(year))
    day = case (day > days) do 
            true -> days
            false -> day
          end
    day = day + interval(interval, :day) + interval(interval, :week) * 7
    adjust({:date, year, month, day})
  end

  def tzdump_size(c, time_t) do
    long_t = 4
    <<"TZif2"::binary, 0::integer-size(15)-unit(8),
      tzh_ttisgmtcnt::integer-size(1)-unit(32), 
      tzh_ttisstdcnt::integer-size(1)-unit(32), 
      tzh_leapcnt::integer-size(1)-unit(32), 
      tzh_timecnt::integer-size(1)-unit(32), 
      tzh_typecnt::integer-size(1)-unit(32), 
      tzh_charcnt::integer-size(1)-unit(32), 
      rest::binary>> = c
    timecounts_size=tzh_timecnt*time_t
    ttinfo_size = long_t+1+1
    ttinfo_array_size=tzh_typecnt*ttinfo_size
    leap_array_size = tzh_leapcnt*(time_t+long_t)
    IO.inspect [time_t: time_t, rest: rest, 
                tzh_ttisgmtcnt:      tzh_ttisgmtcnt,
                tzh_ttisstdcnt:      tzh_ttisstdcnt,
                tzh_leapcnt:      tzh_leapcnt,
                tzh_timecnt:      tzh_timecnt,
                tzh_typecnt:      tzh_typecnt,
                tzh_charcnt: tzh_charcnt,
                timecounts_size: timecounts_size
               ]

    << timecounts::binary-size(timecounts_size), 
    ttinfo_indexs::binary-size(tzh_timecnt), 
    ttinfo_arrays::binary-size(ttinfo_array_size),
    ttabbr_arrays::binary-size(tzh_charcnt),
    rest::binary >> = rest
    IO.inspect [
      timecounts:      timecounts,
      ttinfo_indexs:      ttinfo_indexs,
      ttinfo_arrays:      ttinfo_arrays,
      ttabbr_arrays:      ttabbr_arrays,
#      leap_arrays:      leap_arrays,
      thz_leapcnt: tzh_leapcnt,
      leap_array_size2: tzh_leapcnt*(time_t+long_t),
      leap_array_size: leap_array_size,
      rest_size: byte_size(rest),
      rest: rest]
    << leap_arrays::binary-size(leap_array_size),
       rest:: binary>> = rest
    IO.inspect [
      leap_arrays:      leap_arrays,
      rest: rest]
    
    <<
      standard_wall_arrays::binary-size(tzh_ttisstdcnt),
      utc_local_arrays::binary-size(tzh_ttisgmtcnt),
      rest::binary>> = rest
    transition = for << i::integer-signed-size(time_t)-unit(8) <- timecounts >>, do: i 
    transitionc = for << i::binary-size(8) <- timecounts >>, do: i 
    ttinfo_idx = for << i::integer-size(1)-unit(8) <- ttinfo_indexs >>, do: i 

    ttinfo = for << i::binary-size(ttinfo_size) <- ttinfo_arrays >> do
        << tt_gmtoff::integer-signed-size(long_t)-unit(8),
           tt_isdst::integer-size(1)-unit(8),
           tt_abbrind::integer-unsigned-size(1)-unit(8)>> = i
           %{tt_gmtoff: tt_gmtoff,
             tt_isdst: tt_isdst,
             tt_abbrind: tt_abbrind}
    end
    leap = for << leap_time::integer-signed-size(time_t)-unit(8), 
                  leap_offset::integer-signed-size(long_t)-unit(8) <- leap_arrays >> do
        %{leap_time: leap_time, leap_offset: leap_offset}
    end
    standard_wall = for << i::integer-size(1)-unit(8) <- standard_wall_arrays >>, do: i
    utc_local = for << i::integer-size(1)-unit(8) <- utc_local_arrays >>, do: i
    {%{
       tzh_ttisgmtcnt: tzh_ttisgmtcnt,
       tzh_ttisstdcnt: tzh_ttisstdcnt,
       tzh_leapcnt: tzh_leapcnt,
       tzh_timecnt: tzh_timecnt,
       tzh_typecnt: tzh_typecnt,
       tzh_charcnt: tzh_charcnt,
       transition: transition,
       transitionc: transitionc,
       ttinfo_idx: ttinfo_idx,
       ttinfo: ttinfo,
       leap: leap,
       ttabbr_arrays: ttabbr_arrays,
       standard_wall: standard_wall,
       utc_local: utc_local}, rest}
  end
  defp days_to_month_dayp([], {mm, dd}) do
    {mm, dd}
  end
  defp days_to_month_dayp([h|t], {mm, dd}) do
    case (y = (dd - h)) > 0 do
      true -> 
        days_to_month_dayp(t, {mm + 1, y})
      false ->
        {mm, dd}
    end
  end
  def days_to_month_day(days, is_leap, acc \\ 1) do
    months = [[31,28,31,30,31,30,31,31,30,31,30,31],
              [31,29,31,30,31,30,31,31,30,31,30,31]]
    m = case is_leap do
          true -> Enum.at(months, 1)
          false -> Enum.at(months, 0)
        end
#    IO.inspect [days_to_month: [days, is_leap, m, is_leap]]
    days_to_month_dayp(m, {1, days+1})
  end
  def detect_leap(t, leaps) do
    Enum.reduce_while(leaps, {%{leap_time: 0, leap_offset: 0},
                              %{leap_time: 0, leap_offset: 0}, t}, 
                      fn(e, {ok, ok2, oa}) ->
                        cond do
                          (ok.leap_time >= oa) ->
                            {:halt, {ok, ok2, ok2}}
                          (e.leap_time >= oa) ->
                            {:halt, {ok, ok2, ok}}
                          true -> 
                            {:cont, {e, ok, oa}}
                        end
                      end)
  end
  @dayofyear 365
  @day4year (@dayofyear*3+(@dayofyear+1))
  @day100year @day4year*24+@dayofyear*4
  @day400year @day100year*3+@day4year*25
  @posix_epoch :calendar.datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
  def serial_to_utc(t, leaps) do
    {leap_sec, last_leap, _} = detect_leap(t, leaps)
    loffset = Map.get(leap_sec, :leap_offset)
    leap_time = Map.get(leap_sec, :leap_time)
    direction = loffset - Map.get(last_leap, :leap_offset)
    day4year = @day4year
    day100year = @day100year 
    day400year = @day400year
    rest = t - loffset
    IO.inspect [t: t, leap_time: leap_time, loffset: loffset, direction: direction, last_leap: last_leap]
    {sec, rest, carry}  = case (t == leap_time) do
                     true ->
                       case direction do
                        m when m > 0 -> 
                         {60, rest - 60, 1}
                        m when m < 0 ->
                         {58, rest - 58, 1}
                       end
                     false ->
                       {rem(rest, 60), rest, 0}
                   end
    {sec, rest}  = if (sec < 0) do
      {sec + 60, rest - 60}
    else
      {sec, rest}
    end
    rest = rest + @posix_epoch
    # rest = rest + :calendar.datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
    rest = div(rest,60)+carry
    min = rem(rest, 60)
    {min, rest}  = if (min < 0) do
      {min + 60, rest - 60}
    else
      {min, rest}
    end
    rest = div(rest, 60)
    hour = rem(rest, 24)
    {hour, rest} = if (hour < 0) do 
      {24+hour, rest - 24}
    else
      {hour, rest}
    end
    days = div(rest, 24)
    year400 = div(days, day400year)
    mod400 = rem(days, day400year)
    year100 = year400*400+div(mod400, day100year)*100
    mod100 = rem(mod400, day100year)
    year4 = year100 + div(mod100, day4year)*4
    mod4 = rem(mod100, day4year)
    {y4, m4} = case (mod4 < 0) do
                 true -> {-4, mod4 + day4year}
                 false -> {0, mod4}
               end
    year4 = year4 + y4
    is_leap = (m4 < 366) 
    {year, days} = case is_leap do
                     true -> 
                       {year4 + 0, m4}
                     false ->
                       m5 = m4 - @dayofyear - 1
                       {year4 + 1 + div(m5, @dayofyear),
                        rem(m5, @dayofyear)}
                   end
#    IO.inspect [year4: year4, m4: m4, days: days]
    {month, day} = days_to_month_day(days, is_leap)
    {{year, month, day},{hour, min, sec}}
  end

  def tzdump(file) do
    {:ok, c} = File.read(file)
    {structure4, rest} = tzdump_size(c, 4)
#    IO.inspect [structure4: structure4, rest: rest]
    m = Enum.zip(structure4.transition, structure4.ttinfo_idx)
    m2 = Enum.map(m, fn({t, i}) ->
      ttinfo = Enum.at(structure4.ttinfo, i)
      a = structure4.ttabbr_arrays
      abbr = binary_part(a, ttinfo.tt_abbrind, byte_size(a) - ttinfo.tt_abbrind) |>
             String.split("\0") |>
             Enum.at(0)
      t = serial_to_utc(t, structure4.leap)
      {t, Enum.at(structure4.ttinfo,i), abbr}
    end)
#    IO.inspect [m2: m2]
    {structure8, rest} = tzdump_size(rest, 8)
#    IO.inspect [structure8: structure8, rest: rest]
    m = Enum.zip(structure8.transition, structure8.ttinfo_idx)
#    IO.inspect [m: m]
    m2 = Enum.map(m, fn({t, i}) ->
      ttinfo = Enum.at(structure8.ttinfo, i)
      a = structure8.ttabbr_arrays
      abbr = binary_part(a, ttinfo.tt_abbrind, byte_size(a) - ttinfo.tt_abbrind) |>
             String.split("\0") |>
             Enum.at(0)
      t = serial_to_utc(t, structure8.leap)
      IO.inspect [m2: t]
      {t, Enum.at(structure8.ttinfo,i), abbr}
    end)
    m3 = Enum.map(structure8.leap, 
                 fn(x) -> [offset: x.leap_offset, leap_time: serial_to_utc(x.leap_time, structure8.leap), raw_leap_time: x.leap_time]  end
    )
    [m2, m3]
  end
end
