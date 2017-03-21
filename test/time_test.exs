defmodule TimeTest do
  use ExUnit.Case, async: false
  @moduletag :timetest
  require ExTime
  import ExTime

  test "add_year" do
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: 4)
    m = date(year: 2016, month: 1, day: 1)
    assert {:date, 2020, 1, 1} == add(m, a)
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: -20, month: 2, day: -1)
    m = date(year: 2016, month: 1, day: 1)
    assert {:date, 1996, 2, 29} == add(m, a)
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: 0, month: -2, day: 0)
    m = date(year: 2016, month: 1, day: 1)
    assert {:date, 2015, 11, 1} == add(m, a)
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: 0, month: -2, day: -1)
    m = date(year: 2016, month: 1, day: 1)
    assert {:date, 2015, 10, 31} == add(m, a)
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: 0, month: -10, day: -1)
    m = date(year: 2016, month: 1, day: 1)
    assert {:date, 2015, 2, 28} == add(m, a)
    m = date(year: 2016, month: 1, day: 1)
    a = interval(year: -3, month: -10, day: -1)
    assert {:date, 2012, 2, 29} == add(m, a)
    assert {:date, 2012, 2, 29} == add(date(year: 2012, month: 1, day: 31), interval(month: 1))
    assert {:date, 2012, 3, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 2))
    assert {:date, 2012, 4, 30} == add(date(year: 2012, month: 1, day: 31), interval(month: 3))
    assert {:date, 2012, 5, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 4))
    assert {:date, 2012, 6, 30} == add(date(year: 2012, month: 1, day: 31), interval(month: 5))
    assert {:date, 2012, 7, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 6))
    assert {:date, 2012, 8, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 7))
    assert {:date, 2012, 9, 30} == add(date(year: 2012, month: 1, day: 31), interval(month: 8))
    assert {:date, 2012, 10, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 9))
    assert {:date, 2012, 11, 30} == add(date(year: 2012, month: 1, day: 31), interval(month: 10))
    assert {:date, 2012, 12, 31} == add(date(year: 2012, month: 1, day: 31), interval(month: 11))
    assert {:date, 2013, 2, 1} == add(date(year: 2012, month: 1, day: 31), 
                                      interval(month: 12, day: 1))
  end
  test "timetest" do
    
  end
end
