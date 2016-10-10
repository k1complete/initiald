defmodule TutorialDTest do
  use ExUnit.Case
  import InitialD
  
  test "union" do
    assert Set.equal?(union(Enum.into([1,2,3],HashSet.new),
                            Enum.into([4,5,6],HashSet.new)),
                      Enum.into([1,2,3,4,5,6], HashSet.new))
  end
  test "minus" do
    assert Set.equal?(minus(Enum.into([1,2,3],HashSet.new),
                            Enum.into([1,2,6],HashSet.new)),
                      Enum.into([3], HashSet.new))
  end
  test "intersect" do
    assert Set.equal?(intersect(Enum.into([1,2,3], HashSet.new),
                                Enum.into([2,3,4], HashSet.new)),
                      Enum.into([2,3], HashSet.new))
  end
  test "project" do
    r = Enum.into([%{name: "a", age: 20, sex: :man},
                   %{name: "b", age: 30, sex: :woman},
                   %{name: "c", age: 40, sex: :man}], HashSet.new)
    
    assert Set.equal?(project(r, [:name, :sex]),
                      Enum.into([%{name: "a", sex: :man},
                                 %{name: "b", sex: :woman},
                                 %{name: "c", sex: :man}], HashSet.new))
  end
  test "where" do
    r = Enum.into([%{name: "a", age: 20, sex: :man},
                   %{name: "b", age: 30, sex: :woman},
                   %{name: "c", age: 40, sex: :man}], HashSet.new)
    assert where(r, fn(x) -> x[:age] == 20 end) == 
      Enum.into([%{name: "a", age: 20, sex: :man}], HashSet.new)
    
  end
  test "join" do
    left = Enum.into([%{name: "a", age: 20, sex: :man},
                      %{name: "b", age: 30, sex: :woman},
                      %{name: "c", age: 40, sex: :man}], HashSet.new)
    right = Enum.into([%{age: 10, gen: :teen},
                       %{age: 20, gen: :young},
                       %{age: 40, gen: :elder}], HashSet.new)
    assert join(left, right) == 
      Enum.into([%{name: "a", age: 20, sex: :man, gen: :young},
                 %{name: "c", age: 40, sex: :man, gen: :elder}
                ], HashSet.new)
    
  end
  test "matching" do
    left = Enum.into([%{name: "a", age: 20, sex: :man},
                      %{name: "b", age: 30, sex: :woman},
                      %{name: "c", age: 40, sex: :man}], HashSet.new)
    right = Enum.into([%{age: 10, gen: :teen},
                       %{age: 20, gen: :young},
                       %{age: 40, gen: :elder}], HashSet.new)
    assert matching(left, right) == 
      Enum.into([%{name: "a", age: 20, sex: :man},
                 %{name: "c", age: 40, sex: :man}
                ], HashSet.new)
  end
  test "divideby" do
    left = [%{father: "Hans", mother: "Helga", child: "Harald", age: 5},
            %{father: "Hans", mother: "Helga", child: "Maria",  age: 4},
            %{father: "Hans", mother: "Ulsula", child: "Sabine",  age: 2},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Maria",  age: 4},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Sabine",  age: 2},
            %{father: "Peter", mother: "Christina", child: "Robert",  age: 9}]
    left = Enum.into(left, HashSet.new)
    right = Enum.into([%{child: "Maria", age: 4},
                       %{child: "Sabine", age: 2}], HashSet.new)
    assert divideby(left, right) == 
      Enum.into([%{father: "Martin", mother: "Melanie"}
                ], HashSet.new)
    
  end
  test "rename" do
    left = [%{father: "Hans", mother: "Helga", child: "Harald", age: 5},
            %{father: "Hans", mother: "Helga", child: "Maria",  age: 4},
            %{father: "Hans", mother: "Ulsula", child: "Sabine",  age: 2},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Maria",  age: 4},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Sabine",  age: 2},
            %{father: "Peter", mother: "Christina", child: "Robert",  age: 9}]
    left = Enum.into(left, HashSet.new)
    right = Enum.into([%{child: "Maria", age: 4},
                       %{child: "Sabine", age: 2}], HashSet.new)
    left2 = rename(left, [{:father, :grand}])
    assert divideby(left2, right) == 
      Enum.into([%{grand: "Martin", mother: "Melanie"}
                ], HashSet.new)
  end
  test "extend_add" do
    left = [%{father: "Hans", mother: "Helga", child: "Harald", age: 5},
            %{father: "Hans", mother: "Helga", child: "Maria",  age: 4},
            %{father: "Hans", mother: "Ulsula", child: "Sabine",  age: 2},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Maria",  age: 4},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Sabine",  age: 2},
            %{father: "Peter", mother: "Christina", child: "Robert",  age: 9}]
    left = Enum.into(left, HashSet.new)
    left2 = extend_add(left, [upper: fn(x) -> Map.get(x, :age) * 2.5 end])
    rs = project(left2, [:upper])
    rh = Enum.into([%{upper: 12.5},
                    %{upper: 10.0},
                    %{upper: 5.0},
                    %{upper: 17.5},
                    %{upper: 22.5}], HashSet.new)
    assert Set.equal?(rs, rh) == true
  end
  test "update" do
    left = [%{father: "Hans", mother: "Helga", child: "Harald", age: 5},
            %{father: "Hans", mother: "Helga", child: "Maria",  age: 4},
            %{father: "Hans", mother: "Ulsula", child: "Sabine",  age: 2},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Maria",  age: 4},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Sabine",  age: 2},
            %{father: "Peter", mother: "Christina", child: "Robert",  age: 9}]
    left = Enum.into(left, HashSet.new)
    y = where(left, 
              fn(x) -> x[:father] == "Peter" end)
    r4 = extend_add(y, [new_age: fn(_e) -> 10 end]) |>
      project([:father, :mother, :child, :new_age]) |>
      rename([{:new_age, :age}])
    s = update(left, union(minus(left, y), r4))
    assert s == Enum.into([%{father: "Peter", 
                            mother: "Christina",
                            child: "Robert",
                            age: 10}], HashSet.new)
    s2 = update(left, where: &(&1[:father] == "Peter"),
                       set: [age: fn(_x) -> 10 end])
    assert s == s2
  end
  test "delete" do
    left = [%{father: "Hans", mother: "Helga", child: "Harald", age: 5},
            %{father: "Hans", mother: "Helga", child: "Maria",  age: 4},
            %{father: "Hans", mother: "Ulsula", child: "Sabine",  age: 2},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Maria",  age: 4},
            %{father: "Martin", mother: "Melanie", child: "Gertrud",  age: 7},
            %{father: "Martin", mother: "Melanie", child: "Sabine",  age: 2},
            %{father: "Peter", mother: "Christina", child: "Robert",  age: 9}]
    left = Enum.into(left, HashSet.new)
    af = delete(left, where: &(&1[:father] == "Peter"))

    assert af == Enum.into([%{father: "Peter", 
                              mother: "Christina", 
                              child: "Robert", age: 9}], HashSet.new)
  end
end
