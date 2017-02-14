(* 
**
** Author: Eray Ozkural <eray.ozkural@gmail.com>
**
** Copyright (C) 2011-2017 Gok Us Sibernetik Ar&Ge Ltd.
**
** This program is free software; you can redistribute it and/or modify it under** the terms of the Affero GNU General Public License as published by the Free
** Software Foundation; either version 3 of the License, or (at your option)
** any later version.
**
** Please read the COPYING file.
**
*)

open Printf

let index x dv i =
  Array.iter
    (fun dvelt->
       i.(dvelt.term) <- SparseVector.append i.(dvelt.term) (x,dvelt.freq)
    ) dv.vector

let all_pairs_vertical =
  let o = ref Docmatch_List.empty in
  let m = max_dim v  in (* maximum dimension in data set *)
    (* inverted lists *)
  let v = Util.map Dv.normalize v in 
  let i = Array.init (m+1) (fun ix->SparseVector.make ()) in 
  let id = ref 0 in
    List.iter
      (fun dv ->
         o := Docmatch_List.union !o (find_matches_0 !id x i t);
         index !id dv i;
         id := !id + 1
      ) v;
    !o (* return output set*)
