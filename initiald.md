* [relational][elixir][tutorial_d] tutorial-Dをelixirで実装してみる

tutorial-Dという言語を知っている人はあまりいないでしょうが、クリス・デ
イトという人なら知っている人も多いでしょう。tutorial-Dとは、クリス・デ
イトが提案している(SQLとは違う)「真のリレーショナルな」言語です。型とタ
プル、関係と関係変数と関係代数、そして若干の集約関数と参照整合性制約で
構成されています。

リレーショナルデータベースを行と列で構成された2次元の構造を基として、そ
れを表といい、表と表の間の関係を表現出来ることから、関係データベース
（リレーショナルデータベース）というのだ、という説明は、さすがに最近で
は減ってきましたが、これは大嘘です。

n項のある特定の関係とは、n次元空間の特定の位置ベクトルを指します。この
n次元空間の名称が関係名で、単にn項関係とも言います。

各項には、定義域があり、その定義域の数をm(n)とすると、n項の関係には、
Π(m(n))だけの組み合わせが考えられ、これが、ヴィトゲンシュタイン的にい
う「可能な事態」(可能なは冗長ですが)となります。関係中の各位置ベクトル
は、「事実、つまり成立している事態」の集合をなします。つまり、関係は、
ヴィトゲンシュタイン的にいう、「世界の論理的像」になるわけです。時折デー
タベース界隈でヴィトゲンシュタインが突如登場するのは、上記のような対応
関係があるからなのです(特にNULL否定派に夛いかも)。

関係代数は関係に成立する演算を定義したもので、全体の体系としては一階述
語論理のサブセットと等価です。そして一階述語論理は完全かつ健全であるこ
とが証明されています。

where relname, fn(t) ->
      t[:s] == 1
end

このマクロを

  attindex = get_index(relname.attributes, :s)
  Bs = erl_eval:add_binding(:R, relname.query, erl_eval:new_bindings());
  q = :qlc.string_to_handle('[X || X <- R, element(#{attindex}, X) == 1].', Bs)
  %{rename | :query => q}

的に展開してほしい。やる事は、

  fn の引数 {{:., [Access, get]}を見付けて、その第一引数が fnの引数と同じ
  なら、
  quote bindings: [s: :s, relname: relname] do
   'element(#{Relval.ai(relname, s)}, X)'
  と出来る。
  あとは、Macro.to_string(M, &fmt/2)を使ってフォーマットする。
  という処理をMacro.prewalkerに仕込む

  するとASTが文字列になる。
  そして、これらを「実行時」に行うために、whereマクロではさらにescape する。

  defmacro where(left, binding \\ [], exp) do
    m = Macro.escape(exp)
    quote bind_quoted: [left: left, exp: exp, binding: binding] do
      %__MODULE__{left | query: do_while(left, binding, exp)}
    end
  end
  def do_while(left, binding, exp) do
    {:fn, _, 
      [{:->, _, [[{x, _, _}]], body]}]} = exp
    bs = [{x, left}|binding] 
    Macro.prewalk(body,
      fn({{:., [], [Access, :get]}, [], [arg1, arg2]}) ->
        case arg1 do        
          {^x, _, atom} when is_atom(a) and is_atom(atom) -> 
            quote do: element(Relval.ai(unquote(left), arg2), unquote(left))
          arg1 ->
            arg1
        end 
       (x) -> x
     end)
     
    {:fn, _, 
     [{:->, [],
       [[{x, [], Elixir}], ## ここまで固定で仮引数xを捕捉するし、
                           ## bindingリストにいれておく
        {:==, [], 
          [{{:., [], [Access, :get]}, [], [{:t, [], Elixir}, :s]}, 1]}]}]}
     :->/2の第２引数からprewalkする
    [x: left | binding]
    Access.get({:x, [], Elixir}, Y)なら element(Relval.ai(x, Y), x)
    
  end
    
