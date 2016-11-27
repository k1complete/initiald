ヒストリテーブル

関係変数 R={K, V} について変更を記録する方法として
いくつかの方法を検討する。
1. 期間 duration=[S, E) を追加する。

  R = {K, V, [S, E)} primary key (K, S)

  この方法だと、Kを被参照キーとして使うときに一意性がないため
  通常のDBMSでは使えない。残念。

2. 期間 duration=[S, E) を追加するが、キーはそのまま

  R = {K, V, [S, E)} primary key (K)
  
  同じ構成の関係変数を作成し、そちらに時刻情報付のキーと
  する。

  H = {K, V, [S, E)} primary key (K, S)

  Rへのinsert/update/delete時にafter triggerでHへの
  insertとupdateを行うようにする。

  