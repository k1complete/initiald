履歴の考えかた

関連は事実の集合である。つまり、時間軸上変化する可能
性のあるものはある時刻での事実ということになる。つま
りある事実Aについては、時刻Tで成り立つのなら、{A, T}
ということになる。

さて時がながれてT2では、Aの一部が変更され、A2となった
{A2, T2}となるが、AとA2は要件上「同じもの」と扱わなけ
ればならない場合、どうするか、というものが関連の変更
履歴の問題となる。

AとA2の間で、「同じもの」とみなすのであれば、時の流れ
のなかでも絶対に変化しない属性を探し、それをAKとして、
その他の属性集合をAVとするならば、{AK,AV,T}という形で
ある特定の時刻の事実を記録できそうである。


ところが、この関連の候補キーは{AK, T}なので(AVは同じ
値にまた戻るかもしれない。また、同じ時刻にAVが異る値
であることは有り得ない)、{{AK, T}, AV}のようにかいて
おく。


一方、Aを外部キーとしてもつ関連Bがあったとして、外部
キーをAとしたとして、同様に{{BK, T}, BV, A}のようにか
けたとする。このAは、AKにするべきだろうか、{AK, T}だ
ろうか。結論は、AKが望ましいはずである。

その時点での{AK, T}を実際に求めてそれをBに{AK,T}とし
て記述しても良さそうだが、その後、Bに関係しない変更が
Aに当った場合はBも変更しなければならなくなってしまう。

つまり、{BK, T}を記録した時点Tでの最新版とおもわれる
AKの存在を要求していると見るべきであろう。そして時刻
TでのAの状態の計算は結合時に行うべきであろう。

つまり、外部キーとして使うときには通常の主キーとして
記述してよい。しかし、通常のDBMSでは外部キーの被参照
キーは一意性制約が必要である。

さてさらに時がながれ、過去のT0の情報を取り出す必要が
当った場合、どうするか。

A: {{AK, T}, AV} 
B: {{BK, T}, BV, AK}

となっていたとして、

(A where T =:= T0){not T} join (B where T=:=T0){not T}

SQLだと、

select * from 
  (select AK,AV from A where T=:=T0) AT 
  join 
  (select BK,BV,AK from B where T=:=T0) BT

で取り出すことができると嬉しい。ただし、T=:=T0は
T=<T0となる最大のTという意味である。
R where T=:=T0は

R join (sumarize R where T =< T0 PER (R{AK}) ADD max(T) as T)

SQLだと、

select * from R join on R.T = MR.MT and R.AK = MR.MT 
       (select AK, max(T) as MT from R
        where T <= T0 group by AK) MR

で計算できる。group byしてjoinが必要なのでかなり面倒
ではある。これらば関数として実装しておけばよいだろう。

一方、各レコードに有効期間を使う方法もあるだろう。

A: {{AK, T, TE}, AV}

TEはレコードの終了日である。有効なレコードでは
TEは:infinityなどのマークとなるだろう。この方法であれ
ば、ただの範囲検索でT=:=T0を計算することができる。

問題は、TEは候補ーキであるが、候補キーが変更されるこ
とと(これは候補キーとして扱わず、ただの属性とみなすこ
とで回避できる)、実は次のレコードの開始日と同一である
必要があり、冗長であることである(これもトリガーなどで
手間を回避できるが、情報自体の冗長性は除去できない)。
ここは冗長性は必要ないので、TEを使わない方法でいって
みよう。また、さすがに性能の問題も考えられるので、
属性(TE)として扱う場合についても、折りにふれてみる。
TEは初期値として:infinityという値が入っていて、如何なる
時刻よりも大きいとする。

R where T=:=T0

はただの範囲検索となる。

R where T <= T0 and TE >= T0

更新は、insertとupdateになる。

update R where K=AK and T <= now and TE >= now, TE=now,
insert R relation (tuples, T=now)

削除についてはどうだろう。
ある時点で存在しなくなったという事実を表現するために
先のモデルを修正する。

A: {{AK, T, E}, AV}
B: {{BK, T, E}, BV, AK}

Eは存在していたことを示す真偽値をとる。これがfalseだと
Tの時点から存在しなくなったということを示す。
俗にいう削除日と削除フラグになるが、これが候補キーとなって
いることがポイントである。
オペレータ =:= は、Eを扱うように拡張され

(R join (sumarize R where T =< T0 PER (R{AK}) ADD max(T) as T)) 
   where E=true

SQLだと

select * from R join on R.T = MR.MT and R.AK = MR.MT 
       (select AK, max(T) as MT from R
        where T <= T0 group by AK) MR
    where E.E = true

となる。これでその時T0のEの値によっては、満されるものが
ないかもしれなくなった。

TEを使う場合はどうなるだろうか。やはり更新と同様だが、
insertが無い形になろう。

update R where K=AK and T <= now and TE >= now, TE=now

これで時刻nowでKがAKであるレコードが削除された
ことになる。

次に、BからみたAKの参照整合性制約はどうなるだろう。任
意の時刻T0における参照整合性制約を満せばいいので、常
に

subset((A where T =:= T0){AK}, (B where T=:=T0){AK})

T0でのAのAKの集合 ⊇ BのAKの集合が成り立てばいい。

そのような制約を手動で作るのは大変だが、外部キー側や
主キー側のテーブルのinsertトリガ(削除もinsertになるの
で)を工夫することでエラーを作ることが出来るだろう。

TEを使う場合はupdateトリガとなるだろう。insertは
updateのafterトリガとして動作させることになろう。

トリガの引数は限られているため、時刻情報はnowを使うし
かないのが辛いところだが、通常のDBMSはトランザクショ
ン開始時刻を一環して提供してくれるので大丈夫だろう。

因みに、復活(一時は存在しなかったものが、時刻T3で再び
出現してそれが、以前と同じものだという場合)も、実は簡
單に表現できる。つまり、時刻T3でEがtrueとしてinsertす
るだけである。

閉世界仮説

DBは成立する事実を格納し、格納されていないことをもっ
て偽とみなすという、閉世界仮説を取り入れている。(実際
はNULLがあるため、このような2VLではなく3VLとなってい
て、それが混乱の元となっているがそれは別の話)

履歴情報を格納することによって、DBが世界を理解するた
めの情報が増えたように思えるが、本当だろうか。
従来の設計A1と、時刻情報をもったA2とで比較してみよう。

A1: {AK, AV}
A2: {{AK, T, E}, AV}

について、Aの中にAK1が存在していないことを計算するのは、

A1: sumarize A1 where AK = AK1, count() = 0

    SQLでは
    select (select count(AK) from A1 where AK=AK1) = 0

A2: sumarize A1 where AK = AK1, T=:=T0, count() = 0

このように、何をするにもまずT0での存在する事実集合を
計算してから他の計算を行うため、時間軸方向への拡張が
されてはいるものの、閉世界仮説を逸脱してはいない。

ただし、EやTの情報を直接使うようにすると、閉世界仮説
が破れてしまうので注意が必要である。

たとえば、Eがfalseの集合を集めても、「存在しない事物
の集合」にはならないし、その補集合を求めても「存在す
る事物の集合」とは限らない。E==falseが意味するのは、
Tを組合せたときにその時刻で削除されたということだけで
ある。DBに格納された後に削除された物の集合や、その補
集合には意味がない。

2VLと3VL

会社のプロジェクトで好きにさせて上げようと、相談されない
かぎりほっておいたらNULLがやたらと濫用されていて溜らなく
なったのでデトックスのために記述している。

RDBでは稀本的には閉世界仮説を取り入れている。だから、
入っていないものは「無い」という立場である。一方、
NULLはその値が不明であるというマークである。0でも真で
も偽でもなくあらゆる値でもない。したがって、通常の
演算子を適用すると、大変なことになる。

  真をT
  偽をF
  NULLをU (unknown)

と略すと、基本論理演算の真偽値表は以下のようになる。

AND 

  |T U F 
 -------
 T|T U F
 U|U U F
 F|F U F

OR

  | T U F 
 -------
 T| T T F
 U| U U F
 F| F U F

NOT

  | T U F 
 --------
  | F U T

ここまではまぁそうだろうと思われるだろう。だが、排中
律(A)OR(NOT(A))が恒真にならないというのは、古典命題論
理としては致命傷で、直観論理の枠内でなんとかしないと
いけなくなる。

しかも問題はここからで、通常の算術演算についてもNULLは
定義されていて、

plus/minus/multiply/divide/

   | U 
 ------
 N | U

つまり、NULLがどこかに入ると式全体がunknownになってし
まう。group byとか始めると、もうどうしようもない。と
にかくNULLを使ってはいけない。つかうなら、is null or
is not nullのみで使うべきだし、それならただの必須真偽
値フィールドとして定義しておけばいい。なぜそれをしな
いのか。