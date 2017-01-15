defmodule Relval.Assign.Util do
  def old_and_new_tuples(rel, exp, bind, result) do
#    IO.inspect [old_and_new_tuples: bind, exp: exp]
#    old = Reltuple.raw_new(nr, rel.types)
    :qlc.fold(fn(t, acc) ->
#      IO.inspect [fold: t, exp: exp, rel: rel, type: rel.types]
      old = Reltuple.raw_new(t, rel.types)
#      IO.inspect [old: old, bind: bind, exp: exp]
      {r, _o} = Code.eval_quoted(exp, [old: old] ++ bind)
#      IO.inspect [r: r]
      new_candidate = Enum.reduce(r, old, fn({k, nv}, a) ->
        {old_att, new_val} = Reltuple.get_and_update(a, k, fn(x) ->
#          IO.inspect [a: a, k: k, v: nv, x: x, r: r]
          case Reltype.validate(a.types[k], nv) do
            true -> 
              {x, nv}
            _ -> 
              raise(RelType.TypeConstraintError, 
                    [type: a.types[k], 
                      value: nv,
                      attribute: k])
          end
        end)
        new_val
      end)
      key = Enum.reduce(rel.keys, {}, 
                      fn(x, a) -> 
                        Tuple.append(a, new_candidate[x]) 
                      end)
      new_tuple = :erlang.setelement(2, new_candidate.tuple, key)
      new = Reltuple.raw_new(new_tuple, rel.types)
#      IO.inspect [new: new]
      acc = if (elem(old.tuple, 1) === elem(new.tuple, 1)) do
        acc
      else
        Map.put(acc, :deletes, [old | acc[:deletes]] )
      end
#      IO.inspect [acc: acc]
      acc = Map.put(acc, :updates, [new | acc[:updates]])
#      IO.inspect [acc2: acc]
      acc
    end, result, rel.query)
  end
end
defmodule Relval.Assign do
  defmacro assign(bind, block) do
#    IO.inspect [block: block]
    [do: blocks] = block
#    IO.inspect [blocks: blocks]
    tuples = Enum.reduce(blocks, [], fn(b, acc) ->
      {:__block__, _, r} = 
        case b do
          {:"->", _, [[[update: rel]], keyword]} ->
#            IO.inspect [keyword: keyword]
            exp = Macro.escape(keyword)
            quote bind_quoted: [rel: rel, keyword: exp, bind: bind] do
              acc = Map.put(acc, :relations, 
                            MapSet.put(acc[:relations], rel.name))
              nr = Qlc.q("""
              [X || X <- Q]
              """, [Q: rel])
#              IO.inspect [keyword2: Macro.escape(keyword), rel: rel]
              acc = Relval.Assign.Util.old_and_new_tuples(rel, keyword, 
                                                          bind, acc)
            end
          {:"->", _, [[[delete: rel]], _keyword]} ->
            quote bind_quoted: [rel: rel] do
              nr = Qlc.q("""
              [{element(1, X), element(2, X)} || X <- Q]
              """, [Q: rel.query])
              e = Qlc.e(nr)
              acc = Map.put(acc, :relations, 
                            MapSet.put(acc[:relations], rel.name))
              acc = Map.put(acc, :deletes, e ++ acc[:deletes])
            end
          {:"->", _, [[[insert: rel]], newrel]} ->
            quote bind_quoted: [rel: rel, newrel: newrel] do
              table = rel.name
              keys = rel.keys
              acc = Map.put(acc, :relations, 
                            MapSet.put(acc[:relations], table))
              nr = Qlc.q("""
              [erlang:setelement(1, erlang:setelement(2, X, F(X)), N) ||
              X <- Q]
              """, [Q: newrel.query, 
                    F: fn(x) -> 
                      :erlang.element(2, x)
                    end,
                    N: table])
              e = Qlc.e(nr)
#              IO.inspect [e: e]
              acc = Map.put(acc, :updates, e ++ acc[:updates] )
            end
        end
      acc ++ r
    end)
#    IO.inspect [tuple: tuples]
    
    ret = quote do
      acc = %{updates: [], deletes: [], relations: MapSet.new()}
      #IO.inspect [tuples: tuples]
      try do
        unquote_splicing(tuples)
      catch 
        e, m ->
          :mnesia.abort({e, m})
      end
        
      t = acc
#      IO.inspect [t222: t]
      deletes = t[:deletes]
      if (nil != deletes and !Enum.all?(deletes, fn(rec) ->
#            IO.inspect [deletes: rec]
            table = elem(rec.tuple, 0)
            key = elem(rec.tuple, 1)
            :ok == :mnesia.delete({table, key})
          end)) do
#        IO.inspect [NG: t]
        :mnesia.abort(:delete_abort)
      end
      updates = t[:updates]
#      IO.inspect [updates: updates]
      if (!Enum.all?(updates, fn(rec) ->
#            IO.inspect [updates: rec]
            :ok == :mnesia.write(rec.tuple)
          end)) do
#        IO.inspect [NG: t]
        :mnesia.abort(:update_abort)
      end
      ret =Constraint.validate(Enum.to_list(t[:relations]))
#      IO.inspect [ret: ret, rel: Enum.to_list(t[:relations])]
      if (ret != :ok) do
#        IO.inspect [NG: ret]
        :mnesia.abort(:constraint_abort)
      end
#     IO.inspect [t: t]
      t
    end
#    IO.puts Macro.to_string(ret)
    ret
  end
end
