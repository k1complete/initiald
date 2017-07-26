defmodule ZicTest do
  use ExUnit.Case, async: false
  use ExTime
  @moduletag :zic
  test "read_file" do 
    m = ExZic.read_file("test/fixture/", "asia") 
    assert(m == ["test\n"])
  end

  test "parse_zone" do
    m = """
    Zone America/Chicago	-5:50:36 -	LMT	1883 Nov 18 12:09:24
                                -6:00	US	C%sT	1920
                                -6:00	Chicago	C%sT	1936 Mar  1  2:00
			        -5:00	-	EST	1936 Nov 15  2:00
			        -6:00	Chicago	C%sT	1942
			        -6:00	US	C%sT	1946
			        -6:00	Chicago	C%sT	1967
			        -6:00	US	C%sT
    Zone America/Juneau	 15:02:19 -	LMT	1867 Oct 18
    			 -8:57:41 -	LMT	1900 Aug 20 12:00
    			 -8:00	-	PST	1942
    			 -8:00	US	P%sT	1946
    			 -8:00	-	PST	1969
    			 -8:00	US	P%sT	1980 Apr 27  2:00
    			 -9:00	US	Y%sT	1980 Oct 26  2:00
    			 -8:00	US	P%sT	1983 Oct 30  2:00
    			 -9:00	US	Y%sT	1983 Nov 30
    			 -9:00	US	AK%sT
    """
    a = String.split(m, "\n")

    ret = [
      %{name: "America/Chicago", record: :zone, zone: 
        [%{format: "LMT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 5, 0, 36, 0}}, 
           rules: "-", 
           until: {{1883, 11, 18}, %{standard: "w", time: {12, 0, 24}}}, 
           from: {{:min, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "US", 
           until: {{1920, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1883, 11, 18}, %{standard: "w", time: {12, 0, 24}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "Chicago", 
           until: {{1936, 3, 1}, %{standard: "w", time: {2, 0, 0}}}, 
           from: {{1920, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "EST", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0}}, 
           rules: "-", 
           until: {{1936, 11, 15}, %{standard: "w", time: {2, 0, 0}}}, 
           from: {{1936, 3, 1}, %{standard: "w", time: {2, 0, 0}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "Chicago", 
           until: {{1942, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1936, 11, 15}, %{standard: "w", time: {2, 0, 0}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "US", 
           until: {{1946, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1942, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "Chicago", 
           until: {{1967, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1946, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "C%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0}}, 
           rules: "US", 
           until: {{:max, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1967, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}]}, 
      %{name: "America/Juneau", record: :zone, zone: 
        [%{format: "LMT", 
           gmtoff: {1, {:interval, 0, 0, 0, 0, 0, 0, 0, 15, 0, 19, 0}}, 
           rules: "-", 
           until: {{1867, 10, 18}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{:min, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "LMT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 41, 0}}, 
           rules: "-", 
           until: {{1900, 8, 20}, %{standard: "w", time: {12, 0, 0}}}, 
           from: {{1867, 10, 18}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "PST", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0}}, 
           rules: "-", 
           until: {{1942, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1900, 8, 20}, %{standard: "w", time: {12, 0, 0}}}}, 
         %{format: "P%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0}}, 
           rules: "US", 
           until: {{1946, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1942, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "PST", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0}}, 
           rules: "-", 
           until: {{1969, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1946, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "P%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0}}, 
           rules: "US", 
           until: {{1980, 4, 27}, %{standard: "w", time: {2, 0, 0}}}, 
           from: {{1969, 1, 1}, %{standard: "w", time: {0, 0, 0}}}}, 
         %{format: "Y%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0}}, 
           rules: "US", 
           until: {{1980, 10, 26}, %{standard: "w", time: {2, 0, 0}}}, 
           from: {{1980, 4, 27}, %{standard: "w", time: {2, 0, 0}}}}, 
         %{format: "P%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0}}, 
           rules: "US", 
           until: {{1983, 10, 30}, %{standard: "w", time: {2, 0, 0}}}, 
           from: {{1980, 10, 26}, %{standard: "w", time: {2, 0, 0}}}}, 
         %{format: "Y%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0}}, 
           rules: "US", 
           until: {{1983, 11, 30}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1983, 10, 30}, %{standard: "w", time: {2, 0, 0}}}}, 
         %{format: "AK%sT", 
           gmtoff: {-1, {:interval, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0}}, 
           rules: "US", 
           until: {{:max, 1, 1}, %{standard: "w", time: {0, 0, 0}}}, 
           from: {{1983, 11, 30}, %{standard: "w", time: {0, 0, 0}}}}]}, ""]
    assert ret == ExZic.parse(a) 
  end 
  test "rule_parse" do 
    a = ["Rule Japan 1948 only - May Sun>=1 2:00 1:00 D", 
         "Rule Japan 1948 1951 - Sep Sat>=8 2:00 0 S", 
         "Rule Japan 1949 only - Apr Sun>=1 2:00 1:00 D", 
         "Rule Japan 1950 1951 - May Sun>=1 2:00 1:00 D"] 

    s = [%{record: :rule, name: "Japan", from: 1948, to: 1948, type: "-", 
           in: 5, on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7},
           at: %{standard: "w", time: {2,0,0}}, 
           save: ExTime.interval(hour: 1), letter_s: "D"}, 
         %{record: :rule, name: "Japan", from: 1948, to: 1951, type: "-", 
           in: 9, on: %{day: 8, op: &ExZic.ge/2, prep: nil, weeknum: 6}, 
           at: %{standard: "w", time: {2,0,0}}, 
           save: ExTime.interval(hour: 0), letter_s: "S"}, 
         %{record: :rule, name: "Japan", from: 1949, to: 1949, type: "-", 
           in: 4, on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           at: %{standard: "w", time: {2,0,0}}, 
           save: ExTime.interval(hour: 1), letter_s: "D"}, 
         %{record: :rule, name: "Japan", from: 1950, to: 1951, type: "-", 
           in: 5, on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           at: %{standard: "w", time: {2,0,0}}, 
           save: ExTime.interval(hour: 1), letter_s: "D"}] 

    assert s == ExZic.parse(a) 
  end 
  test "parse_america" do
    # Rule NAME FROM TO	TYPE	IN	ON	AT	SAVE	LETTER/S
    a = [
      "Rule US	1918 1919 -	Mar	lastSun	2:00	1:00	D",
      "Rule US	1918 1919 -	Oct	lastSun	2:00	0	S",
      "Rule US	1942 only -	Feb	9	2:00	1:00	W", # War
      "Rule US	1945 only -	Aug	14	23:00u	1:00	P", # Peace
      "Rule US	1945 only -	Sep	lastSun	2:00	0	S",
      "Rule US	1967 2006 -	Oct	lastSun	2:00	0	S",
      "Rule US	1967 1973 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1974 only -	Jan	6	2:00	1:00	D",
      "Rule US	1975 only -	Feb	23	2:00	1:00	D",
      "Rule US	1976 1986 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1987 2006 -	Apr	Sun>=1	2:00	1:00	D",
      "Rule US	2007 max -	Mar	Sun>=8	2:00	1:00	D",
      "Rule US	2007 max -	Nov	Sun>=1	2:00	0	S"]

    s = [%{at: %{standard: "w", time: {2, 0, 0}}, 
           from: 1918, in: 3, letter_s: "D", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1919, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1918, in: 10, 
           letter_s: "S", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, to: 1919, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1942, 
           in: 2, letter_s: "W", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1942, 
           type: "-", on: %{op: nil, prep: nil, weeknum: nil, day: 9}}, 
         %{at: %{standard: "u", time: {23, 0, 0}}, from: 1945, in: 8, 
           letter_s: "P", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1945, 
           type: "-", on: %{op: nil, prep: nil, weeknum: nil, day: 14}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1945, in: 9, 
           letter_s: "S", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, to: 1945, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1967, in: 10, 
           letter_s: "S", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, to: 2006, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1967, in: 4, 
           letter_s: "D", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1973, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1974, in: 1,
           letter_s: "D", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1974, 
           type: "-", on: %{op: nil, prep: nil, weeknum: nil, day: 6}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1975, in: 2, 
           letter_s: "D", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1975, 
           type: "-", on: %{op: nil, prep: nil, weeknum: nil, day: 23}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1976, in: 4, 
           letter_s: "D", name: "US", record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, to: 1986, 
           type: "-", on: %{day: nil, op: nil, prep: "last", weeknum: 7}}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 1987, in: 4, 
           letter_s: "D", name: "US", 
           on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           record: :rule, save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 
           to: 2006, type: "-"}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 2007, in: 3, 
           letter_s: "D", name: "US", 
           on: %{day: 8, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           record: :rule, save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 
           to: 2037, type: "-"}, 
         %{at: %{standard: "w", time: {2, 0, 0}}, from: 2007, in: 11, 
           letter_s: "S", name: "US", 
           on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           record: :rule, 
           save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 
           to: 2037, type: "-"}]
    assert s == ExZic.parse(a) 
  end
  test "rule_build" do
    s = [%{record: :rule, name: "Japan", from: 1948, to: 1948, type: "-", 
           in: 4, on: %{day: 1, op: &ExZic.ge/2, prep: nil, weeknum: 7}, 
           at: %{standard: "w", time: {2,0,0}}, 
           save: ExTime.interval(hour: 1), letter_s: "D"},
         %{record: :rule, name: "Japan", from: 1948, to: 1951, type: "-", 
           in: 8, on: %{day: 8, op: &ExZic.ge/2, prep: "", weeknum: 6}, 
           at: %{standard: "w", time: {2,0,0}}, save: ExTime.interval(hour: 0), 
           letter_s: "S"},
         %{record: :rule, name: "Japan", from: 1949, to: 1949, type: "-", 
           in: 3, on: %{day: 1, op: &ExZic.ge/2, prep: "", weeknum: 7}, 
           at: %{standard: "w", time: {2,0,0}}, save: ExTime.interval(hour: 1), 
           letter_s: "D"},
         %{record: :rule, name: "Japan", from: 1950, to: 1951, type: "-", 
           in: 4, on: %{day: 1, op: &ExZic.ge/2, prep: "", weeknum: 7}, 
           at: %{standard: "w", time: {2,0,0}},
           save: ExTime.interval(hour: 1), 
           letter_s: "D"}]
    %{rule: ret, zone: zone} = ExZic.build(s)
    assert hd(s) == hd(ret)
  end
  @tag :find_rule
  test "find_rule" do
    # Rule NAME FROM TO	TYPE	IN	ON	AT	SAVE	LETTER/S
    a = [
      "Rule US	1918 1919 -	Mar	lastSun	2:00	1:00	D",
      "Rule US	1918 1919 -	Oct	lastSun	2:00	0	S",
      "Rule US	1942 only -	Feb	9	2:00	1:00	W", # War
      "Rule US	1945 only -	Aug	14	23:00u	1:00	P", # Peace
      "Rule US	1945 only -	Sep	lastSun	2:00	0	S",
      "Rule US	1967 2006 -	Oct	lastSun	2:00	0	S",
      "Rule US	1967 1973 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1974 only -	Jan	6	2:00	1:00	D",
      "Rule US	1975 only -	Feb	23	2:00	1:00	D",
      "Rule US	1976 1986 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1987 2006 -	Apr	Sun>=1	2:00	1:00	D",
      "Rule US	2007 max -	Mar	Sun>=8	2:00	1:00	D",
      "Rule US	2007 max -	Nov	Sun>=1	2:00	0	S"]
    rules = ExZic.parse(a)
    """
    ret = ExZic.find_rule(rules, {{1918, 1, 1}, {0,0,0}})
    assert ret == %{letter: "S", save: 0}
    ret = ExZic.find_rule(rules, {{1918, 3, 1}, {0,0,0}})
    assert ret == %{letter: "S", save: 0}
    """
    """
    ret = ExZic.find_rule(rules, {{1918, 3, 31}, {0,0,0}})
    assert ret == {1918, 3, 31, {24, %{at: %{standard: "w",
                                             time: {2, 0, 0}},
                                       from: 1918, 
                                       in: 3,
                                       letter_s: "D", 
                                       name: "US",
                                       on: %{day: nil, op: nil,
                                             prep: "last", weeknum: 7},
                                       record: :rule,
                                       save: {:interval, 
                                              0, 0, 0, 0, 0, 
                                              0, 0, 1, 0, 0, 0}, 
                                       to: 1919,
                                       type: "-"}}}
    ret = ExZic.find_rule(rules, {{1919, 3, 31}, {0,0,0}})
    assert ret == 
      {1919, 3, 31,
       {30,
        %{at: %{standard: "w", time: {2, 0, 0}}, from: 1918, in: 3,
          letter_s: "D", name: "US",
          on: %{day: nil, op: nil, prep: "last", weeknum: 7},
          record: :rule,
          save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 
          to: 1919,
          type: "-"}}}
    """
    ret = ExZic.find_rule(rules, {1918, 10, 31})
    assert ret == 
      {1918, 10, 31, {27, 
       %{at: %{standard: "w", time: {2, 0, 0}}, 
         from: 1918, in: 10, letter_s: "S", name: "US", 
         on: %{day: nil, op: nil, prep: "last", weeknum: 7}, 
         record: :rule, 
         save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 
         to: 1919, type: "-"}}}
    """
    ret = ExZic.find_rule(rules, {{1919, 3, 30}, {0,0,0}})
    assert ret == 
      {1919, 3, 30,
       {30,
        %{at: %{standard: "w", time: {2, 0, 0}}, from: 1918, in: 3,
          letter_s: "D", name: "US",
          on: %{day: nil, op: nil, prep: "last", weeknum: 7},
          record: :rule,
          save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 
          to: 1919,
          type: "-"}}}
    """
  end
  
  @tag :find_rule_next_year
  test "find_rule_next_year" do
    # Rule NAME FROM TO	TYPE	IN	ON	AT	SAVE	LETTER/S
    a = [
      "Rule US	1918 1919 -	Mar	lastSun	2:00	1:00	D",
      "Rule US	1918 1919 -	Oct	lastSun	2:00	0	S",
      "Rule US	1942 only -	Feb	9	2:00	1:00	W", # War
      "Rule US	1945 only -	Aug	14	23:00u	1:00	P", # Peace
      "Rule US	1945 only -	Sep	lastSun	2:00	0	S",
      "Rule US	1967 2006 -	Oct	lastSun	2:00	0	S",
      "Rule US	1967 1973 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1974 only -	Jan	6	2:00	1:00	D",
      "Rule US	1975 only -	Feb	23	2:00	1:00	D",
      "Rule US	1976 1986 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1987 2006 -	Apr	Sun>=1	2:00	1:00	D",
      "Rule US	2007 max -	Mar	Sun>=8	2:00	1:00	D",
      "Rule US	2007 max -	Nov	Sun>=1	2:00	0	S"]
    rules = ExZic.parse(a) |> ExZic.build_transition()
#    IO.inspect([rules: rules])
    ret = ExZic.find_rule(rules, {1919, 3, 29})
    assert ret == 
      { {1918, 10, 27},
       %{at: %{standard: "w", time: {2, 0, 0}}, 
        from: 1918, in: 10, letter_s: "S", 
        on: %{day: nil, op: nil, prep: "last", weeknum: 7}, 
        name: "US", record: :rule, 
        save: {:interval, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 
        to: 1919, type: "-"}}
    ret = ExZic.find_rule(rules, {1918, 1, 1})
    assert ret == 
      %{at: 0, from: 0, in: 1, letter_s: nil, on: 0}
    ret = ExZic.find_rule(rules, {1917, 1, 1})
    assert ret == 
      %{at: 0, from: 0, in: 1, letter_s: nil, on: 0}
    ret = ExZic.find_rule(rules, {1918, 10, 26})
    assert ret == {{1918, 3, 31},
                   %{at: %{standard: "w", time: {2, 0, 0}}, from: 1918, in: 3,
                     letter_s: "D", name: "US",
                     on: %{day: nil, op: nil, prep: "last", weeknum: 7},
                     record: :rule,
                     save: {:interval, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 
                     to: 1919,
                     type: "-"}}

  end
  @tag :build_transition2
  test "build_transision" do
    # Rule NAME FROM TO	TYPE	IN	ON	AT	SAVE	LETTER/S
    a = [
      "Rule US	1918 1919 -	Mar	lastSun	2:00	1:00	D",
      "Rule US	1918 1919 -	Oct	lastSun	2:00	0	S",
      "Rule US	1942 only -	Feb	9	2:00	1:00	W", # War
      "Rule US	1945 only -	Aug	14	23:00u	1:00	P", # Peace
      "Rule US	1945 only -	Sep	lastSun	2:00	0	S",
      "Rule US	1967 2006 -	Oct	lastSun	2:00	0	S",
      "Rule US	1967 1973 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1974 only -	Jan	6	2:00	1:00	D",
      "Rule US	1975 only -	Feb	23	2:00	1:00	D",
      "Rule US	1976 1986 -	Apr	lastSun	2:00	1:00	D",
      "Rule US	1987 2006 -	Apr	Sun>=1	2:00	1:00	D",
      "Rule US	2007 max -	Mar	Sun>=8	2:00	1:00	D",
      "Rule US	2007 max -	Nov	Sun>=1	2:00	0	S"]
    b = """
    Zone America/Juneau	 15:02:19 -	LMT	1867 Oct 18
    			 -8:57:41 -	LMT	1900 Aug 20 12:00
    			 -8:00	-	PST	1942
    			 -8:00	US	P%sT	1946
    			 -8:00	-	PST	1969
    			 -8:00	US	P%sT	1980 Apr 27  2:00
    			 -9:00	US	Y%sT	1980 Oct 26  2:00
    			 -8:00	US	P%sT	1983 Oct 30  2:00
    			 -9:00	US	Y%sT	1983 Nov 30
    			 -9:00	US	AK%sT
    """
    s = a ++ String.split(b, "\n")
    ret = ExZic.parse(s) |> 
      ExZic.build() |>
      ExZic.transition()
#    rule = ExZic.split_rule(ret.rule)
#      ExZic.transision()
#    assert ret == nil
#    IO.inspect [ret: ret]
    IO.inspect [rule: ret]
    
#    ExZic.convert({1900, 10, 10, 0, 0, 0}, "America/Juneau")
  end
  
  test "when test" do
    assert ExZic.test(1,2,3) == false
    assert ExZic.test(10,2,3) == true
  end
  test "realrange test" do
    a = RealRange.new(1,10,1,:"[)")
    assert 9 == Enum.count(a)
    a = RealRange.new(1,10,1,:"[]")
    assert 10 == Enum.count(a)
    a = RealRange.new(1,10,1,:"[)")
    assert true == Enum.member?(a, 3.4)
    a = RealRange.new(1,10,1,:"[)")
    assert false == Enum.member?(a, 10)
  end
  test "zellar" do
    assert 6 == ExZic.zellar(2017,4,15) # sat
    assert 7 == ExZic.zellar(2017,4,16) # sun
    assert 7 == ExZic.zellar(2017,1,1)  # sun
    assert 1 == ExZic.zellar(2017,1,2)  # mon
  end
end
