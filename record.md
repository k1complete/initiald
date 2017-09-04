
レコード構成

mnesia は

       {type, k, v,...}

となっている。これならindexを使うことができる。
attributes: [key|attribute_name_list]
user profile = [
     type_map: %{name, type},
     key: key_name_list
     ]
attribute_name_listは、key以外のattributeをリストする。
keyはkey名のリスト
type_mapはattributeとkeyの名前とタイプのマップ

    k = {k1, k2, ...} 

Relvar.create(keyword[name: type], [key])


Relval
%{header => [record_name, [keys], attributes]
  body => [{record_name, key, value....}]
}

