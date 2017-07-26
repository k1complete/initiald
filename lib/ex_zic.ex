defmodule ExZic do
  use ExTime
  def download() do
  end
  @filenames ~w(africa antarctia asia australasia europe northamerica southamerica)
  def read_file(dir, file) do
    {:ok, c} = File.open(Path.join(dir, file))
    ret = IO.stream(c, :line) 
    |> Stream.map(&(Regex.replace(~r/[^"]?#.*\n/, &1, "")))
    |> Stream.filter(&(&1 != ""))
    |> Enum.to_list
    File.close(c)
    ret
  end
  def parse([]) do
    []
  end
  def parse([head|tail]) do
    cond do
      Regex.match?(~r/^Rule\s+/, head) ->
        [rule_parse(head) | parse(tail)]
      Regex.match?(~r/^Link\s+/, head) ->
        [link_parse(head) | parse(tail)]
      Regex.match?(~r/^Zone\s+/, head) ->
        captured = Regex.named_captures(~r/^Zone\s+(?<name>[^\s]+)(?<rest>.*)/, head)
        name = captured["name"]
        rest = captured["rest"]
        {zone, tail} = zone_parse([rest|tail], {{:min, 1, 1}, 
                                                build_at("-")}, [])
        IO.inspect [rec: tail]
        [%{record: :zone, name: name, zone: zone} | parse(tail)]
      true ->
        [head | parse(tail)]
    end
  end
  def link_parse(list) do
  end
  def zone_parse([head|tail], prev_until, zone_info) do
    case Regex.match?(~r/^\s+/, head) do
      false ->
        {Enum.reverse(zone_info), [head|tail]}
      true ->
        zone_match = ~r/\s+
        (?<minus>-)?(?<gmtoff>[^\s]+)\s+
        (?<rules>[^\s]+)\s+
        (?<format>[^\s]+)
        (\s+(?<until_year>[^\s]+)
          (\s+(?<until_month>[^\s]+)\s+(?<until_day>[^\s]+)
            (\s+(?<until_time>[^\s]+))?
          )?
        )?
        /x
        IO.puts "[" <> head <> "]"
        captured = Regex.named_captures(zone_match, head)
        name = captured["name"]
        direction = case default(captured["minus"], "", 1) do
                      "-" -> -1
                      1 -> 1
                    end
        gmtoff = build_save(default(captured["gmtoff"], nil, ""))
        rules = captured["rules"]
        from = prev_until
        format = captured["format"]
        until_year = build_year(default(captured["until_year"], "", "max"))
        until_month = build_month(default(captured["until_month"], "", "Jan"))
        until_day = build_day(default(captured["until_day"], "", "1"))
        until_time = build_at(default(captured["until_time"], "", "00:00:00"))
        until = {{until_year, until_month, until_day}, until_time}
        zone_parse(tail, until,
          [%{gmtoff: {direction, gmtoff},
             rules: rules,
             from: from,
             format: format,
             until: until,
            }| zone_info])
    end
  end
  def build_type("-") do
    "-"
  end
  def build_month(month) do
    m = [~r/^Jan(uary)?$/i, ~r/^Feb(rary)?$/i, ~r/^Mar(ch)?$/i, 
         ~r/^Apr(il)?$/, ~r/^May$/i, ~r/Jun(e)?/i, 
         ~r/^Jul(y)?$/i, ~r/^Aug(ust)?$/, ~r/^Sep(tember)?$/i, 
         ~r/^Oct(ober)?$/i, ~r/^Nov(ember)?$/i, ~r/^Dec(ember)?$/i]
    Enum.find_index(m, &(Regex.match?(&1, month)))+1
  end
  def build_year("only", from) do
    from
  end
  @maxforward 30
  def build_year("max", from) do
    from + @maxforward
  end
  def build_year(to, from) do
    String.to_integer(to)
  end
  def build_year("max") do
    :max
  end
  def build_year(from) do
    String.to_integer(from)
  end
  def build_day(from) do
    String.to_integer(from)
  end
  def build_time(from) do
    
  end
  def build_on(str) do
    at_match = ~r/
      ((?<prep>last|first)?
       (?<week>Sun(day)?|
         Mon(day)?|Tue(sday)?|Wed(nesday)?|
         Thu(rsday)?|Fri(day)?|Sat(urday)?))?
      (?<op>\>=|\<=)?
      (?<day>\d+)? |
      (?<atday>\d+)
      /xi
    captured = Regex.named_captures(at_match, str)
    m = [
         ~r/^Mon(day)?$/i,
         ~r/^Tue(sday)?$/i,
         ~r/^Wed(nesday)?$/i,
         ~r/^Thu(rsday)?$/i,
         ~r/^Fri(day)?$/i,
         ~r/^Sat(urday)?$/i,
         ~r/^Sun(day)?$/i]
    prep = default(captured["prep"], "", nil)
    op = case default(captured["op"], "", nil) do
           ">=" -> &__MODULE__.ge/2
           "<=" -> &__MODULE__.le/2
           nil -> nil
         end
    
#    IO.inspect [on: str, week: captured["week"], prep: captured["prep"]]
#  weeknum: 1 monday, ... 7 sunday
    week = default(captured["week"], "", nil)
    weeknum = case week do
                nil -> 
                  nil
                x -> 
                  Enum.find_index(m, 
                    fn(x) -> 
                      Regex.match?(x, captured["week"]) 
                    end)+1
              end
    day = default(captured["day"], "", nil)
    atday = default(captured["atday"], nil, "1")
    %{op: op,
      day: case prep || weeknum || op || day do
             nil -> 
               String.to_integer(atday)
             _ -> 
               case day do 
                 nil -> 
                   nil
                 y -> 
                   String.to_integer(y)
               end
           end,
      weeknum: weeknum,
      prep: prep
    }
  end
  def default(m, n, d) do
    case m do 
      ^n -> d
      nil -> d
      m -> m
    end
  end
  def build_save(str) do
    at_match = ~r/^(?<hour>\d+)
                   (:(?<minitute>\d+)(:(?<second>\d+))?)?
                   (?<standard>[wsuz])?$|
                  ^(?<hyphen>-)$/x
    captured = Regex.named_captures(at_match, str)
    ExTime.interval(hour: String.to_integer(default(captured["hour"], "", "0")),
         minute: String.to_integer(default(captured["minute"], "", "0")),
         second: String.to_integer(default(captured["second"], "", "0")))
  end
  def build_at(str) do
    at_match = ~r/^(?<hour>\d+)
                   (:(?<minitute>\d+)(:(?<second>\d+))?)?
                   (?<standard>[wsuz])?$|
                  ^(?<hyphen>-)$/x
    captured = Regex.named_captures(at_match, str)
    time = case captured["hyphen"] do
             "-" -> {0,0,0}
             _ ->
               {String.to_integer(default(captured["hour"], "", "0")),
                String.to_integer(default(captured["minute"], "", "0")),
                String.to_integer(default(captured["second"], "", "0"))}
           end
    %{time: time,
      standard: default(captured["standard"], "", "w")}
  end
  def rule_parse(line) do
    rule_match = ~r/Rule[\s]+
      (?<name>[^\s]+)\s+
      (?<from>[^\s]+)\s+
      (?<to>[^\s]+)\s+
      (?<type>[^\s]+)\s+
      (?<in>[^\s]+)\s+
      (?<on>[^\s]+)\s+
      (?<at>[^\s]+)\s+
      (?<save>[^\s]+)\s+
      (?<letter_s>[^\s]+)
      /x
    IO.puts line
    captured = Regex.named_captures(rule_match, line)
    from = build_year(captured["from"])
    %{record: :rule,
      name: captured["name"],
      from: from,
      to: build_year(captured["to"], from),
      type: build_type(captured["type"]),
      in: build_month(captured["in"]),# monthname to monthindex
      on: build_on(captured["on"]), # ((last|first)[:weekday]|[:weekday](>=|<=)\d+|\d+)
      at: build_at(captured["at"]),
      save: build_save(captured["save"]), # HH:MM:SSw
      letter_s: default(captured["letter_s"], "-", "")}
  end
  def build([%{:record => :rule} = head|tail], m) do
    build(tail, %{m | rule: [head|m.rule]})
  end
  def build([%{:record => :zone} = head|tail], m) do
#    IO.inspect [zone_m: m, zone_head: head]
#    build(tail, %{m | zone: [head|m.zone]})
    build(tail, %{m | zone: Map.put(m.zone, head.name, head)})
  end
  def build([""|tail], m) do
    build(tail, m)
  end
  def build([], m) do
    %{rule: Enum.reverse(m.rule), zone: m.zone}
  end
  def build(list) do
    ret = build(list, %{rule: [], zone: %{}})
    %{ret | rule: split_rule(ret.rule)}
  end
  def le(a,b) do
    a <= b
  end
  def ge(a,b) do
    a >= b
  end
  def day_of_week(y,m,d) do
    zeller(y,m,d)
  end
  def zeller(y, m, d) do
    mm = case m do
           1 -> 13
           2 -> 14
           _ -> m
         end
    yy = rem(y, 100)
    c = div(y, 100)
    gamma = cond do
      4 <= y and y <= 1582 ->
        6*c + 5
      1582 < y ->
        5*c + div(c,4)
    end
    rem((d + div(26*(mm + 1), 10) + yy + div(yy, 4) + gamma + 5), 7) + 1
  end

  def transition_day_of_month_from_on(y, m, on) do
    dom = ExTime.day_of_month(m, ExTime.is_leap_year(y))
    case on.prep do
      "first" ->
        wn = zeller(y, m, 1)
        x =  rem(on.weeknum - wn + 7, 7) + 1
      "last" ->
        wn = zeller(y, m, dom)
        x =  dom - (rem(on.weeknum + wn + 7, 7) + 0)
#        IO.inspect [tdom: wn, weeknum: on.weeknum, dom: dom, y: y, x: x]
        x
      nil ->
        cond do
          is_integer(on.day) ->
            on.day
        end
         
    end
  end
  def build_transition([h|t], acc \\ :orddict.new()) do
#    IO.inspect [bt: h]
    x = transition_day_of_month_from_on(h.from, h.in, h.on)
    if (h.from > h.to) do
      build_transition(t, acc)
    else
      h2 = Map.put(h, :from, h.from + 1)
      build_transition([h2|t], :orddict.store({{h.from, h.in, x}, h.at}, h, acc))
    end
  end
  def build_transition([], acc  ) do
    acc
  end
  def split_rule(rule) do
    Enum.reduce(rule, %{}, fn(x, acc) ->
      Map.update(acc, x.name, [x], &([x|&1]))
    end) |>
    Enum.into(%{}, fn({k,v}) -> 
      {k, Enum.reverse(v)}
    end)
  end
    
  def find_rule(rule, {{y, m, d}, at}) do
    r = :orddict.fold(fn(k, v, a) -> 
      case k <= {{y, m, d}, at.time, at.standard} do
        true -> [{k, v}|a]
        false -> a
      end
    end, [%{at: 0, from: 0, in: 1, letter_s: nil, on: 0}], rule) 
#    IO.inspect [r: r]
    hd(r)
  end
  def transision(%{rule: rule, zone: zones}) do
    rule_new = build_transition(rule)
    IO.inspect [rule_new: rule_new]
    Enum.map(zones, fn(z) ->
      {z.name, Enum.reduce(z.zone, [], fn(e, acc) ->
        case e.rules do
          "-" -> 
#            IO.inspect [e: e]
            acc ++[%{format: e.format, gmtoff: e.gmtoff, 
                     rules: e.rules,
                     from: e.from, until: e.until}]
          r -> 
            rule_found = find_rule(rule_new, e.from)
            IO.inspect [new_rule: rule_found]
            acc ++ [rule_found]
        end
      end)}
    end)
  end
  def compile(list) do
    %{rule: rules, zone: zones} = build(list, %{rule: [], zone: []})
  end
  def start() do
    read_file(".", "asia")
    |> parse() 
    |> compile()
  end
  def test(a, b, c) when a >= b + c 
    do
    true
  end
  def test(a, b, c) when a < b + c 
    do
    false
  end

end
