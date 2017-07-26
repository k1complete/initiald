defmodule RealRange do

  defstruct first: nil, last: nil, step: nil, border: nil
  @type t :: RealRange.t
  @type border :: :"[]" | :"[)" 
  @spec new(any, any, any, border) :: t
  def new(first, last, step, border \\ :"[)") do
    %__MODULE__{first: first, last: last, step: step, border: border}
  end
end
defimpl Enumerable, for: RealRange do
  def count(realrange) do
    {:error, __MODULE__}
  end
  def member?(realrange, e) do
    ret =case realrange.border do
      :"[]" ->
        realrange.first <= e and e <= realrange.last 
      :"[)" ->
        realrange.first <= e and e < realrange.last 
    end
    # {:error, __MODULE__}
    {:ok, ret}
  end
  def reduce(_, {:halst, acc}, _fun) do
    {:halted, acc}
  end
  def reduce(realrange, {:suspend, acc}, fun) do
    {:suspended, acc, &reduce(realrange, &1, fun)}
  end

  def reduce(%{first: first, last: last, step: step, border: :"[)"}, {:cont, acc}, fun) when first + step >= last do
    {_a, acc} = fun.(first, acc)
    {:done, acc}
  end
  def reduce(%{first: first, last: last, step: step, border: :"[]"}, {:cont, acc}, fun) when first + step > last do
    {_a, acc} = fun.(first, acc)
    {:done, acc}
  end
  def reduce(%{first: first, last: last, step: step, border: b}, {:cont, acc}, fun) do
    reduce(%{first: first+step, last: last, step: step, border: b}, fun.(first, acc), fun)
  end
  
end
