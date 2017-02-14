(* 
**
** Copyright (C) 2011-2017 Gok Us Sibernetik Ar&Ge Ltd.
**
** This program is free software; you can redistribute it and/or modify it under** the terms of the Affero GNU General Public License as published by the Free
** Software Foundation; either version 3 of the License, or (at your option)
** any later version.
**
** Please read the COPYING file.
**
** Author: Eray Ozkural
*)

open Types
open Printf
open Util
open Gc

module Weight = Types.Fix32Weight
module MyDV = Dv.Make (Weight)
module MySV = SparseVector.DynarrayImpl (Weight)

open Weight
open MyDV

let debug = false
let debug2 = false
let profile = false
let nummatch = ref 0
let num_candidates = ref 0

let adjust_gc () = 
  Gc.set {(Gc.get ()) with 
            minor_heap_size=4*1024*1024;
            major_heap_increment=64*1024*1024;
            space_overhead=200
            (*max_overhead=1000000*)
         }

let docmatch_first (x,_,_) = x

let docmatch_second (_,y,_) = y

module Docmatch_Set_Set = Set.Make 
  (struct
     (* two documents are matched with a specified weight *)
     type t = int * int * Weight.t
     let compare x y = compare (docmatch_first x) (docmatch_second y)
   end)

module Docmatch_List = 
  struct 
    type t = int * int * Weight.t
    let empty = []
    let add dm list = dm::list
    let union list1 list2 = 
      let rec loop list1 list2 = 
        match list1 with 
            [] -> list2
          | hd::tl -> loop tl (hd::list2) in
        loop list1 list2
    let cardinal = List.length 
    let iter = List.iter
  end

module Docmatch_Vec = 
  struct 
    type t = int * int * Weight.t
    let make () = Dynarray.make (0,0,Weight.zero)
    let make_n n = Dynarray.make_reserved (0,0,Weight.zero) n
    let add dm vec = Dynarray.append vec dm
    let union v1 v2 =  Dynarray.iter (fun x-> Dynarray.append v1 x) v2; v1
    let cardinal = Dynarray.length
    let iter = Dynarray.iter
    let extend = Dynarray.extend
  end

module Docmatch_Vec_Set = 
  struct 
    type t = int * int * Weight.t
    let make () = Dynarray.make (0,0,Weight.zero)
    let make_n n = Dynarray.make_reserved (0,0,Weight.zero) n
    let empty = ref (make ())
    let add dm vec = Dynarray.append vec dm; vec
    let union v1 v2 =  Dynarray.iter (fun x-> Dynarray.append v1 x) v2; v1
    let cardinal = Dynarray.length
    let iter = Dynarray.iter
  end

module Docmatch_Vec_Iset = 
  struct 
    type t = int * int * Weight.t
    let make () = Dynarray.make (0,0,Weight.zero)
    let make_n n = Dynarray.make_reserved (0,0,Weight.zero) n
    let add dm vec = Dynarray.append vec dm 
    let cardinal = Dynarray.length
    let iter = Dynarray.iter
  end

(* for 32-bit 
module Docmatch_Vec_32bit = 
struct 
  type t = int * int * Weight.t
  let make () = 
    (Dynarray1.make Bigarray.int, 
     Dynarray1.make Bigarray.int,
     Dynarray1.make Bigarray.int)
  let make_n n = 
    (Dynarray1.make_reserved Bigarray.int n, 
     Dynarray1.make_reserved Bigarray.int n,
     Dynarray1.make_reserved Bigarray.int n)
  let add (x,y,w) (xs,ys,wgts) = 
    Dynarray1.append xs x;
    Dynarray1.append ys y;
    Dynarray1.append wgts w
  let cardinal (xs,ys,wgts) = Dynarray1.length xs
  let iter f (xs,ys,wgts)= 
    for i=0 to (Dynarray1.length xs)-1 do
      f (Dynarray1.get xs i,Dynarray1.get ys i,Dynarray1.get wgts i)
    done
  let extend (xs,ys,wgts) (xs1,ys1,wgts1) = 
    Dynarray1.extend xs xs1;
    Dynarray1.extend ys ys1;
    Dynarray1.extend wgts wgts1
  let union a b = extend a b
end


module Docmatch_Vec_Set_32bit =
struct
  include Docmatch_Vec
  let union a b = Docmatch_Vec.union a b; a (* KLUDGE REMOVE IN THE FUTURE *)
  let add a v = Docmatch_Vec.add a v; v (* KLUDGE REMOVE IN THE FUTURE *)
end
  *)

module Docmatch_Set = Docmatch_Vec_Set
module Docmatch_ISet = Docmatch_Vec

(* maximum dimension in a document vector *)
let max_dim_dv dv = 
  Util.max_list (Array.to_list (Array.map (fun x->x.term) dv.vector)) 0

let max_dim v = 
  Util.max_list (Util.map max_dim_dv v) 0

(* index a list of document vectors
   v = list of document vectors
*)
let index v = 
  let m = max_dim v  in (* maximum dimension in data set *)
    (* inverted lists *)
  (*let v = Util.map MyDV.normalize v in *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in 
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
           ) x.vector;
       ) v;
    if debug2 then 
      (printf "\ni=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    i (* return freshly constructed inverted index *)

(* v = list of document vectors
   t = threshold *)
let all_pairs_bruteforce v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  let va = Array.of_list v in
    Array.iter MyDV.sort va;
    Array.iteri
      (fun i x ->
         for j = 0 to i-1 do
           let y = va.(j) in 
           let w = MyDV.dot_ordered x y in
             if w >= t then
              Docmatch_Vec.add (x.docid,y.docid,w) o  
         done
      ) va;
    o (* return output set*)

let sort_inv_idx idx = 
  Array.sort (fun x y->compare (MySV.length y) (MySV.length x)) idx

let find_matches_0 dv i t =
  let m = Docmatch_Vec.make () in
  let a = Hashtbl.create 10000 in
    if debug then
      (printf "find_matches_0 %g dv = " (Weight.to_float t); MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter 
             (fun (y, w)->
                if Hashtbl.mem a y then
                  Hashtbl.replace a y 
                    ( (Hashtbl.find a y) +: (xw *: w) )
                else
                  Hashtbl.add a y (xw *: w)
             )
             i.(dvelt.term); 
      ) dv.vector;
    Hashtbl.iter
      (fun y w -> 
         if debug then printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
         if w >= t then 
           Docmatch_Vec.add (dv.docid, y, w) m;
      )
      a;
    if profile then num_candidates := !num_candidates + (Hashtbl.length a);
    m
      
(* v = list of document vectors
   t = threshold *)
let all_pairs_0 v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  let m = max_dim v  in (* maximum dimension in data set *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in (* inverted lists *)
    if profile then num_candidates := 0;
    List.iter
      (fun x ->
         Docmatch_Vec.extend o (find_matches_0 x i t);
         Array.iter
           (fun dvelt->
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
           ) x.vector;
      ) v;
    if debug then 
      (printf "\ni=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    if profile then printf "num_candidates=%d\n" !num_candidates;
    o

let find_matches_0_array dv i a t =
  let m = ref (Docmatch_Vec.make ()) in
    if debug then
      (printf "find_matches_0_array dv = "; MyDV.print dv;);
    for ix = 0 to dv.docid-1 do
      a.(ix) <- Weight.zero;
    done;
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter 
             (fun (y, w)-> a.(y) <- a.(y) +: (xw *: w)) i.(dvelt.term); 
      ) dv.vector;
    for y = 0 to dv.docid-1 do
      let w = a.(y) in (
          if debug then printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
          if w >= t then 
            Docmatch_Vec.add (dv.docid, y, w) !m;
          if profile && w > Weight.zero then incr num_candidates;
	)
    done;
    !m
      
(* v = list of document vectors
   t = threshold *)
let all_pairs_0_array v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  let m = max_dim v  in (* maximum dimension in data set *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in  (* inverted index *)
  let n = List.length v in
  let a = Array.make n Weight.zero in
    if profile then num_candidates := 0;
    List.iter
      (fun x ->
         if x.docid mod 100=0 then 
           (printf "doc %d     \r" x.docid; flush stdout);
         Docmatch_Vec.extend o (find_matches_0_array x i a t);
         Array.iter
           (fun dvelt->
              (*printf "%d:%d " dvelt.term x.docid;*)
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
           ) x.vector;
         if debug then 
           (printf "\n*i=\n";
            Array.iteri (fun n x -> Printf.printf "%d: " n; 
                           MySV.print x) i);
      ) v;
    if debug then 
      (printf "\ni=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    if profile then printf "num_candidates=%d\n" !num_candidates;
    o (* return output set*)

let find_matches_0_array2 dv i a t m = 
  let aset = Dynarray.make_reserved 0 (Array.length a) in
    if debug then
      (printf "find_matches_0_array2 dv = "; MyDV.print dv;);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter (fun (y, w)-> 
			if a.(y)=Weight.zero then
			  Dynarray.append aset y;
			a.(y) <- a.(y) +: (xw *: w)) i.(dvelt.term); 
      ) dv.vector;
    Dynarray.iter 
      (fun y ->
         let w = a.(y) in (
             if debug then 
               printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
             if w >= t then 
               Docmatch_ISet.add (dv.docid, y, w) m
	   );
           a.(y) <- Weight.zero (* ready a for the next use *)
      ) aset
      
(* v = list of document vectors
   t = threshold *)
let all_pairs_0_array2 v t = 
  let t = Weight.of_float t in
  let m = max_dim v  in (* maximum dimension in data set *)
    (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in 
  let n = List.length v in
  let m = Docmatch_Vec.make_n (10*n) in 
  let a = Array.make n Weight.zero in
    List.iter
      (fun x ->
         find_matches_0_array2 x i a t m;
         Array.iter
           (fun dvelt->
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
           ) x.vector;
       ) v;
    if debug then 
      (printf "\ni=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    m (* return output set*)

let find_matches_1 dv order v' i t =
  let x = dv.docid in
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) in
    if debug then
      (printf "find_matches_1 %d " x; printf "dv = "; MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           if debug then printf "term %d, weight %f\n" 
             dvelt.term (Weight.to_float dvelt.freq);
           MySV.iter 
             (fun (y, w)->
                if debug then Printf.printf "(%d, %f) " y (Weight.to_float w);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +: xw *: w)
                else
                  Hashtbl.add a y (xw *: w)
             )
             i.(dvelt.term);
           if debug then Printf.printf "\n";
      ) dv.vector;
    if debug then (printf "|a| = %d\n" (Hashtbl.length a));
    Hashtbl.iter
      (fun y sim -> 
         if debug then printf "y=%d, sim=%f\n" y (Weight.to_float sim);
         let s = sim +: (MyDV.dot dv v'.(y) order) in 
           if debug then Printf.printf "Sim of %d and %d = %f\n" x y 
             (Weight.to_float s); 
           if  s >= t then
             Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    if profile then num_candidates := !num_candidates + (Hashtbl.length a);
    !m
  
let max_weight_dv dv =
  max_list (Array.to_list (Array.map (fun x->x.freq) dv.vector)) Weight.zero

let maxweight v = 
  max_list (Util.map max_weight_dv v) Weight.zero
    
let calc_dim_nz v m = 
  let dim_nz = Array.make m 0 in
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              dim_nz.(dvelt.term) <- dim_nz.(dvelt.term) + 1 
           ) x.vector
      ) v;
    dim_nz

(* reorder dimensions in place *)
let reorder_dims v m = 
  let dim_nz = calc_dim_nz v m in
  let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz)
  and order = Array.make m 0 in
  let sorted_dims = 
    List.sort (fun a b -> compare (snd b) (snd a)) dims in
    (*printf "sorted_dims = ";*)
    List.iter2 (fun i x ->
                  order.(fst x) <- i;
                  (*Printf.printf "(%d,%d) " (fst x) (snd x) *)
               ) (Util.range 0 (m-1)) sorted_dims;
    (*
      Printf.printf "\norder = ";
      Array.iter (fun x->Printf.printf "%d " x) order; Printf.printf "\n";
    *) 
    List.iter (fun x ->
                 let xvp = x.vector in 
                   Array.sort (fun a b -> compare order.(a.term) 
                                 order.(b.term)) xvp) v;
    order

let maxweight_dim_preprocess v m = 
  let maxwgt_dim = Array.make m Weight.zero in 
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              maxwgt_dim.(dvelt.term) <- 
                (max maxwgt_dim.(dvelt.term) dvelt.freq)
           ) x.vector
      ) v;
    maxwgt_dim

let minweight_dim_preprocess v m = 
  let minwgt_dim = Array.make m (Weight.of_float 1.0) in 
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              minwgt_dim.(dvelt.term) <- 
                (min minwgt_dim.(dvelt.term) dvelt.freq)
           ) x.vector
      ) v;
    minwgt_dim

(* v = list of document vectors
   t = threshold *)
let all_pairs_1 v t = 
  let t = Weight.of_float t in
  if debug then (printf "all_pairs_1 v  = \n"; List.iter MyDV.print v);
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v  (* number of dimensions in data set *)
  and n = List.length v in
    (*and v = Util.map MyDV.normalize v in *)
  let maxweight_dim = maxweight_dim_preprocess v m 
  (* inverted lists *)
  and i = Array.init m (fun ix->MySV.make ()) 
  and order = reorder_dims v m 
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in 
    if profile then num_candidates := 0;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; Array.iter (fun x->
                                                   Printf.printf "%f " 
                                                     (Weight.to_float x)) 
         maxweight_dim;
       printf "\n");
    List.iter
      (fun x ->
         let id = x.docid and b = ref Weight.zero and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" id; 
           Docmatch_Vec.extend !o (find_matches_1 x order v' i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then printf "b=%f\n" (Weight.to_float !b);
                if !b >=: t then
                  (if debug then printf "adding (%d, %f)\n" id 
                     (Weight.to_float dvelt.freq);
                   MySV.append i.(dvelt.term) (id, dvelt.freq))
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           v'.(id) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then 
             (printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" id; MyDV.print v'.(id);
             );
      ) v;
    if profile then printf "num_candidates=%d\n" !num_candidates;
    !o (* return output set*)

let find_matches_1_array dv order v' i a t =
  let x = dv.docid 
  and m = ref (Docmatch_Vec.make ()) in
  let aset = Dynarray.make_reserved 0 (Array.length a) in
    if debug then
      (printf "find_matches_1_array %d " x; printf "dv = "; MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter (fun (y, w)-> 
			if a.(y)=Weight.zero then
			  Dynarray.append aset y;
			a.(y) <- a.(y) +: (xw *: w)) i.(dvelt.term); 
      ) dv.vector;
    Dynarray.iter 
      (fun y ->
         let w = a.(y) +: (MyDV.dot dv v'.(y) order) in (
               if debug then 
                 printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
             if w >= t then 
               Docmatch_Vec.add (x, y, w) !m;
	   );
           a.(y) <- Weight.zero  (* ready a for the next use *)
      ) aset; 
    if profile then 
      num_candidates := !num_candidates + (Dynarray.length aset);
    !m
 
(* v = list of document vectors
   t = threshold *)
let all_pairs_1_array v t = 
  let t = Weight.of_float t in
  if debug then (printf "all_pairs_1 v  = \n"; List.iter MyDV.print v);
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v  (* number of dimensions in data set *)
  and n = List.length v in
    (*and v = Util.map MyDV.normalize v in *)
  let maxweight_dim = maxweight_dim_preprocess v m 
  (* inverted lists *)
  and i = Array.init m (fun ix->MySV.make ()) 
  and a = Array.make n Weight.zero
  and order = reorder_dims v m 
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in 
    if profile then num_candidates := 0;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; Array.iter (fun x->
                                                   Printf.printf "%f " 
                                                     (Weight.to_float x)) 
         maxweight_dim;
       printf "\n");
    List.iter
      (fun x ->
         let id = x.docid and b = ref Weight.zero and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" id; 
           Docmatch_Vec.extend !o (find_matches_1_array x order v' i a t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then printf "b=%f\n" (Weight.to_float !b);
                if !b >=: t then
                  (if debug then printf "adding (%d, %f)\n" id 
                     (Weight.to_float dvelt.freq);
                   MySV.append i.(dvelt.term) (id, dvelt.freq))
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           v'.(id) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then 
             (printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" id; MyDV.print v'.(id);
             );
      ) v;
    if profile then printf "num_candidates=%d\n" !num_candidates;
    !o (* return output set*)

let find_matches_2 x dv maxweight_dim order vp vsize i t =
  (* TODO: estimate table size *)
  let tf = Weight.to_float t in
  let a  = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and remscore = ref Weight.zero
  and minsize = (tf /. (Weight.to_float (max_weight_dv dv))) in
    remscore := Array.fold_left (+:) Weight.zero 
      (Array.map (fun x->x.freq *: maxweight_dim.(x.term)) dv.vector);  
    if debug then
      (printf "find_matches_2 %d " x;
       printf "dv = "; MyDV.print dv;);
    Array.iter
      (fun dvelt->
         let ti = dvelt.term and xw = dvelt.freq in
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize  do
               MySV.pop i.(ti) (* TODO: +: epsilon_float*)
           done;
           MySV.iter 
             (fun (y, w)->
                if debug then Printf.printf "(%d, %f) " y (Weight.to_float w);
                if (Hashtbl.mem a y && (Hashtbl.find a y)!=Weight.zero) ||
                  (!remscore >= t) then
                    (if Hashtbl.mem a y then
                       Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: w)
                     else
                       Hashtbl.add a y (xw *: w);
                    )
             )
             i.(dvelt.term);
           remscore := !remscore -: xw *: maxweight_dim.(dvelt.term);
      ) dv.vector;
    Hashtbl.iter
      (fun y w -> 
         if w +: 
           (Weight.of_float 
              (float_of_int(min (MyDV.length vp.(y)) (MyDV.length dv) )))
           *: max_weight_dv(dv) *: max_weight_dv(vp.(y)) >=: t then
             let s = w +: (MyDV.dot dv vp.(y) order) in 
               if debug then Printf.printf "Sim of %d and %d = %f, w=%f\n" x y
                 (Weight.to_float s) (Weight.to_float w); 
               if  s >= t then
                 Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m
  
(* v = list of document vectors
   t = threshold *)

let all_pairs_2 v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v (* number of dimensions in data set *)
  and n = List.length v in
    (*and v = Util.map MyDV.normalize v in*) 
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) 
    (* TODO: change v in place*)
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) in
  let order =  reorder_dims v m in 
  let v = List.sort (fun a b -> compare (max_weight_dv b) (max_weight_dv a)) v
  and vsize = Array.make (List.length v) 0 in
    List.iter
      (fun x->vsize.(x.docid) <- Array.length x.vector) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; Array.iter (fun x->
                                                  Printf.printf "%f " 
                                                    (Weight.to_float x)) 
         maxweight_dim;

       printf "\nvsize = \n"; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         let b = ref Weight.zero 
	 and maxwgtx = max_weight_dv x
         and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" x.docid; 
           Docmatch_Vec.extend !o (find_matches_2 x.docid x maxweight_dim order vp vsize i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: (min maxweight_dim.(dvelt.term) maxwgtx )
                *: dvelt.freq;
                if debug then (Printf.printf "b=%f\n" (Weight.to_float !b));
                if !b >= t then
                  (
                    if debug then printf "adding (%d, %f)\n" x.docid
                      (Weight.to_float dvelt.freq);
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                  )
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then
             (Printf.printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" x.docid; MyDV.print vp.(x.docid))
      ) v;
    !o (* return output set*)


let find_matches_0_remscore dv maxweight_dim order i t =
  (* estimate table size *)
  let x = dv.docid in 
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and remscore = ref Weight.zero in
    remscore := Array.fold_left (+:) Weight.zero 
      (Array.map (fun x->x.freq *: maxweight_dim.(x.term)) dv.vector); 
    if debug then
      (printf "find_matches_0_remscore %d " x;
       printf "dv = "; MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           if debug then printf "term %d, weight %f, remscore %f\n" 
             dvelt.term (Weight.to_float dvelt.freq) 
             (Weight.to_float !remscore); 
           MySV.iter 
             (fun (y, yw)->
                if debug then printf "(%d, %f) " y (Weight.to_float yw);
                if (Hashtbl.mem a y && (Hashtbl.find a y)!=Weight.zero) ||
                  (!remscore (*+: epsilon_float*) >=: t) then
                    (if Hashtbl.mem a y then
                       Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                     else
                       Hashtbl.add a y (xw *: yw);
                    )
             )
             i.(dvelt.term);
           remscore := !remscore -: xw *: maxweight_dim.(dvelt.term);
      ) dv.vector;
    if debug then printf "|a| = %d\n" (Hashtbl.length a);
    Hashtbl.iter
      (fun y s -> 
         if debug then printf "y=%d, score=%f\n" y (Weight.to_float s);
         if  s >= t then
           Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m
  
  
(* v = list of document vectors
   t = threshold 
   remscore optimization *)
let all_pairs_0_remscore v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) 
  and m = 1 + max_dim v  in (* number of dimensions in data set *)
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) in 
  let id = ref 0
    (* TODO: change v in place*)
  and order = reorder_dims v m in 
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; 
       Array.iter (fun x-> Printf.printf "%f " (Weight.to_float x)) 
         maxweight_dim;
       printf "\n");
    List.iter
      (fun x ->
         if debug then printf "processing doc %d\n" !id; 
         Docmatch_Vec.extend !o 
           (find_matches_0_remscore x maxweight_dim order i t);
         Array.iter
           (fun dvelt->
              if debug then printf "checking term %d\n" dvelt.term;
              (*Printf.printf "b=%f\n" !b;*)
              if debug then printf "adding (%d, %f) to II\n" !id 
                (Weight.to_float dvelt.freq);
                MySV.append i.(dvelt.term) (!id, dvelt.freq)
           ) x.vector;
         if debug then 
           (printf "\ni = \n"; 
            Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
              i;
           );
         id := !id + 1
      ) v;
    !o (* return output set*)


let find_matches_1_remscore x dv maxweight_dim order vsize i t =
  (* estimate table size *)
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and remscore = ref Weight.zero in
    remscore := Array.fold_left (+:) Weight.zero 
      (Array.map (fun x->x.freq *: maxweight_dim.(x.term)) dv.vector);  

    if debug then
      (printf "find_matches_2a %d " x;
       printf "dv = "; MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           if debug then 
             printf "term %d, weight %f, remscore %f\n" dvelt.term
               (Weight.to_float dvelt.freq) (Weight.to_float !remscore); 
           MySV.iter 
             (fun (y, yw)->
                if debug then printf "(%d, %f) " y (Weight.to_float yw);
                if (Hashtbl.mem a y && (Hashtbl.find a y)!=Weight.zero) ||
                  (!remscore (*+: epsilon_float*) >=: t) then
                    (if Hashtbl.mem a y then
                       Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                     else
                       Hashtbl.add a y (xw *: yw);
                    )
             )
             i.(dvelt.term);
           remscore := !remscore -: xw *: maxweight_dim.(dvelt.term);
      ) dv.vector;
    if debug then printf "|a| = %d\n" (Hashtbl.length a);
    Hashtbl.iter
      (fun y score -> 
         if debug then printf "y=%d, score=%f\n" y (Weight.to_float score);
         if  score >=: t then
           Docmatch_Vec.add (x, y, score) !m;
      )
      a;
    !m
  
  
(* v = list of document vectors
   t = threshold 
   remscore optimization *)

let all_pairs_1_remscore v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v  in (* number of dimensions in data set *)
  let maxweight_dim = maxweight_dim_preprocess v m 
    (* inverted lists *)
  and i = Array.init m (fun ix->MySV.make ()) 
  and id = ref 0
  and order =  reorder_dims v m in 
  let vsize = Array.make (List.length v) 0 in
    List.iter2 (fun i x->vsize.(i) <- Array.length x.vector) (Util.range 0 ((List.length v)-1)) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; 
       Array.iter (fun x-> Printf.printf "%f " (Weight.to_float x)) 
         maxweight_dim;
       printf "\nvsize = \n"; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         if debug then printf "processing doc %d\n" !id; 
         Docmatch_Vec.extend !o (find_matches_1_remscore !id x maxweight_dim order vsize i t);
         Array.iter
           (fun dvelt->
              if debug then printf "checking term %d\n" dvelt.term;
              if debug then printf "adding (%d, %f) to II\n" !id 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (!id, dvelt.freq)
           ) x.vector;
         if debug then 
           (printf "\ni = \n"; 
            Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
              i;
           );
         id := !id + 1
      ) v;
    !o (* return output set*)


let find_matches_1_upperbound x dv maxweight_dim order vp vsize i t =
  (* TODO: estimate table size *)
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) in
    if debug then
      (printf "find_matches_2 %d " x;
       printf "dv = "; MyDV.print dv);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter 
             (fun (y, w)->
                if debug then Printf.printf "(%d, %f) " y (Weight.to_float w);
                if Hashtbl.mem a y then
                   Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: w)
                else
                  Hashtbl.add a y (xw *: w);
             )
             i.(dvelt.term);
           if debug then Printf.printf "\n";
      ) dv.vector;
    Hashtbl.iter
      (fun y w -> 
         if w +: 
           (Weight.of_float 
              (float_of_int(min (MyDV.length vp.(y)) (MyDV.length dv) )))
           *: max_weight_dv(dv) *: max_weight_dv(vp.(y)) >=: t then
             let s = w +: (MyDV.dot dv vp.(y) order) in 
               if debug then Printf.printf "Sim of %d and %d = %f, w=%f\n" x y 
                 (Weight.to_float s) (Weight.to_float w); 
               if  s >= t then
                 Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m


(* v = list of document vectors
   t = threshold 
   upper bound optimization *)
let all_pairs_1_upperbound v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v (* number of dimensions in data set *)
  and n = List.length v in
  (*and v = Util.map MyDV.normalize v in*)
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) in 
  let id = ref 0
    (* TODO: change v in place*)
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and order =  reorder_dims v m in 
  let vsize = Array.make (List.length v) 0 in
    List.iter2 (fun i x->vsize.(i) <- Array.length x.vector) (Util.range 0 ((List.length v)-1)) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; 
       Array.iter (fun x -> Printf.printf "%f " (Weight.to_float x)) 
         maxweight_dim;

       printf "\nvsize = \n"; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         let b = ref Weight.zero 
	   (*and maxwgtx = max_weight_dv x *)
         and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" !id; 
           Docmatch_Vec.extend !o (find_matches_1_upperbound !id x maxweight_dim order vp vsize i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                (* in original algo: b := !b +: (min maxweight_dim.(dvelt.term) (maxwgtx) )
                 *: dvelt.freq;*)
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then (Printf.printf "b=%f\n" (Weight.to_float !b));
                if !b >= t then
                  (
                    if debug then printf "adding (%d, %f)\n" !id 
                      (Weight.to_float dvelt.freq);
                    MySV.append i.(dvelt.term) (!id, dvelt.freq)
                  )
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           vp.(!id) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then
             (Printf.printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" !id; MyDV.print vp.(!id));
           id := !id + 1;
      ) v;
    !o (* return output set*)

(*
let find_matches_2c2 x dv maxweight_dim order vp vsize i t =
  (* TODO: estimate table size *)
  let (a : (int, float) Hashtbl.t)  = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and minsize = (t/:(max_weight_dv dv)) in
    if debug then
      (printf "find_matches_2c2 %d, length=%f " x (MyDV.magnitude dv);
       printf "dv = "; MyDV.print dv;
       printf "minsize=%f, max_weight_dv=%f\n" minsize (max_weight_dv dv));
    Array.iter
      (fun dvelt->
         let t = dvelt.term and tw = dvelt.freq in
           i.(t) <- List.filter 
             (fun y->
                if debug then
                  printf "y=%d, |y|=%d\n" (fst y) vsize.(fst y);
                (float_of_int vsize.(fst y)) +: epsilon_float>=minsize) i.(t);
           MySV.iter 
             (fun (y, yw)->
                if debug then Printf.printf "(%d, %f) " y yw;
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  tw *: yw)
                else
                  Hashtbl.add a y (tw *: yw);
             )
             i.(dvelt.term);
      ) dv.vector;
    if debug then 
      (printf "\npruned i=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    Hashtbl.iter
      (fun y w -> 
         let s = w +: (MyDV.dot dv vp.(y) order) in 
           if debug then Printf.printf "Sim of %d and %d = %f, w=%f\n" x y s w; 
           if  s >= t then
             m := Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m
  
(* v = list of document vectors
   t = threshold
   minsize optimization, filter all of inverted index *)
let all_pairs_2c2 v t = 
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v (* number of dimensions in data set *)
  and n = List.length v 
  and v = Util.map MyDV.normalize v in 
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) in  
    (* TODO: change v in place*)
  let vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and order =  reorder_dims v m in
  let v = List.sort (fun a b -> compare (max_weight_dv b) (max_weight_dv a)) v 
  and vsize = Array.make (List.length v) 0 in
    List.iter
      (fun x->vsize.(x.docid) <- Array.length x.vector) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; Array.iter (fun x->
                                                  Printf.printf "%f " x) 
         maxweight_dim;

       printf "\nvsize = "; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         let b = ref Weight.zero 
         and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" x.docid; 
           o := Docmatch_Vec.union !o (find_matches_2c2 x.docid x maxweight_dim order vp vsize i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then (Printf.printf "b=%f\n" !b);
                if !b >= t then
                  (
                    if debug then printf "adding (%d, %f)\n" x.docid dvelt.freq;
                    i.(dvelt.term) <-
                      MySV.append i.(dvelt.term) (x.docid, 
                                                          dvelt.freq)
                  )
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then
             (Printf.printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" x.docid; MyDV.print vp.(x.docid))
      ) v;
    !o (* return output set*)
*)

let find_matches_1_minsize x dv maxweight_dim order vp vsize i t =
  (* TODO: estimate table size *)
  let tf = Weight.to_float t in
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and minsize = (tf /. (Weight.to_float (max_weight_dv dv))) in
    if debug then
      (printf "find_matches_1_minsize %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       printf "dv = "; MyDV.print dv;
       printf "minsize=%f, max_weight_dv=%f\n" 
         minsize (Weight.to_float (max_weight_dv dv)));
    Array.iter
      (fun dvelt->
         let t = dvelt.term and tw = dvelt.freq in
	   (*
           i.(t) <- List.filter 
             (fun y->
                if debug then
                  printf "y=%d, |y|=%d\n" (fst y) vsize.(fst y);
                (float_of_int vsize.(fst y)) +: epsilon_float>=minsize) i.(t);
	   *)
           while MySV.length i.(t) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(t) ))) < minsize  do
               MySV.pop i.(t) (* TODO: +: epsilon_float*)
           done;

           MySV.iter 
             (fun (y, yw)->
                if debug then Printf.printf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  tw *: yw)
                else
                  Hashtbl.add a y (tw *: yw);
             )
             i.(dvelt.term);
      ) dv.vector;
    if debug then 
      (printf "\npruned i=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    Hashtbl.iter
      (fun y w -> 
         let s = w +: (MyDV.dot dv vp.(y) order) in 
           if debug then Printf.printf "Sim of %d and %d = %f, w=%f\n" x y
             (Weight.to_float s) (Weight.to_float w); 
           if  s >=: t then
             Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m
  
(* v = list of document vectors
   t = threshold
   minsize optimization, original one *)
let all_pairs_1_minsize v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v (* number of dimensions in data set *)
  and n = List.length v in
  (*and v = Util.map MyDV.normalize v in*) 
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) in  
    (* TODO: change v in place*)
  let vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and order =  reorder_dims v m in
  let v = List.sort (fun a b -> compare (max_weight_dv b) (max_weight_dv a)) v 
  and vsize = Array.make (List.length v) 0 in
    List.iter
      (fun x->vsize.(x.docid) <- Array.length x.vector) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; 
       Array.iter (fun x-> Printf.printf "%f " (Weight.to_float x)) 
         maxweight_dim;

       printf "\nvsize = "; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         let b = ref Weight.zero 
         and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" x.docid; 
           Docmatch_Vec.extend !o (find_matches_1_minsize x.docid x maxweight_dim order vp vsize i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then (Printf.printf "b=%f\n" (Weight.to_float !b));
                if !b >= t then
                  (
                    if debug then printf "adding (%d, %f)\n" x.docid 
                      (Weight.to_float dvelt.freq);
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                  )
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then
             (Printf.printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" x.docid; MyDV.print vp.(x.docid))
      ) v;
    !o (* return output set*)


(* just the minsize optimization *)
let find_matches_0_minsize dv vsize i t =
  (* TODO: estimate table size *)
  let tf = Weight.to_float t in
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and minsize = (tf /. (Weight.to_float (max_weight_dv dv))) in
    if debug then
      (printf "find_matches_0_minsize %d, length=%f " dv.docid 
         (Weight.to_float (MyDV.magnitude dv));
       printf "dv = "; MyDV.print dv;
       printf "minsize=%f, max_weight_dv=%f\n" 
         minsize (Weight.to_float (max_weight_dv dv)));
    Array.iter
      (fun dvelt->
         let t = dvelt.term and tw = dvelt.freq in
           while MySV.length i.(t) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(t) ))) < minsize  do
               MySV.pop i.(t) (* TODO: +: epsilon_float*)
           done;
           MySV.iter 
             (fun (y, yw)->
                if debug2 then printf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  tw *: yw)
                else
                  Hashtbl.add a y (tw *: yw);
             )
             i.(dvelt.term);
      ) dv.vector;
    if debug2 then 
      (printf "\npruned i=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; 
                      MySV.print x) i);
    Hashtbl.iter
      (fun y w -> 
         if debug2 then Printf.printf "s(%d,%d)=%f\n" 
	   dv.docid y (Weight.to_float w); 
         if  w >=: t then
           Docmatch_Vec.add (dv.docid, y, w) !m;
      )
      a;
    !m


(* v = list of document vectors
   t = threshold
   minsize optimization, original one *)
let all_pairs_0_minsize v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v in (* number of dimensions in data set *)
  (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) in   
  let v = List.sort (fun a b -> compare (max_weight_dv b) (max_weight_dv a)) v 
  and vsize = Array.make (List.length v) 0 in
    List.iter
      (fun x->vsize.(x.docid) <- Array.length x.vector) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "\nvsize = "; Array.iter (fun x-> printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         if debug then printf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend !o (find_matches_0_minsize x vsize i t);
         Array.iter
           (fun dvelt->
              if debug2 then 
                (printf "checking term %d, adding (%d, %f)\n" 
                   dvelt.term x.docid (Weight.to_float dvelt.freq));
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
         if debug2 then
           (Printf.printf "\ni = \n"; 
            Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x) i)
      ) v;
    !o (* return output set*)


let find_matches_1_remscore_minsize dv maxweight_dim order vp vsize i t =
  let tf = Weight.to_float t in
  let x = dv.docid in 
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Vec.make ()) 
  and remscore = ref Weight.zero
  and minsize =  (tf /. (Weight.to_float (max_weight_dv dv))) in
    remscore := Array.fold_left (+:) Weight.zero 
      (Array.map (fun x->x.freq *: maxweight_dim.(x.term)) dv.vector);  
    if debug then
      (printf "find_matches_1_remscore_minsize %d " x;
       printf "dv = "; MyDV.print dv;);
    Array.iter
      (fun dvelt->
         let ti = dvelt.term and xw = dvelt.freq in 
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize do
               MySV.pop i.(ti) (* TODO: +: epsilon_float*)
           done;
           MySV.iter 
             (fun (y, w)->
                if debug then Printf.printf "(%d, %f) " y (Weight.to_float w);
                if (Hashtbl.mem a y && (Hashtbl.find a y)!=Weight.zero) ||
                  (!remscore >=: t) then
                    (if Hashtbl.mem a y then
                       Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: w)
                     else
                       Hashtbl.add a y (xw *: w);
                    )
             )
             i.(dvelt.term);
           remscore := !remscore -: xw *: maxweight_dim.(dvelt.term);
      ) dv.vector;
    Hashtbl.iter
      (fun y w -> 
         let s = w +: (MyDV.dot dv vp.(y) order) in 
           if debug then Printf.printf "Sim of %d and %d = %f, w=%f\n" x y
             (Weight.to_float s) (Weight.to_float w); 
           if  s >= t then
             Docmatch_Vec.add (x, y, s) !m;
      )
      a;
    !m
  

(* v = list of document vectors
   t = threshold *)
let all_pairs_1_remscore_minsize v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v (* number of dimensions in data set *)
  and n = List.length v in
  (*and v = Util.map MyDV.normalize v in*) 
  let maxweight_dim = maxweight_dim_preprocess v m in 
    (* inverted lists *)
  let i = Array.init m (fun ix->MySV.make ()) 
    (* TODO: change v in place*)
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) in
  let order =  reorder_dims v m in 
  let v = List.sort (fun a b -> compare (max_weight_dv b) (max_weight_dv a)) v
  and vsize = Array.make (List.length v) 0 in
    List.iter
      (fun x->vsize.(x.docid) <- Array.length x.vector) v;
    if debug then 
      (printf "v = \n"; List.iter MyDV.print v;
       printf "maxweight_dim = \n"; 
       Array.iter (fun x-> Printf.printf "%f " (Weight.to_float x)) 
         maxweight_dim;
       printf "\nvsize = \n"; Array.iter (fun x->
                                            Printf.printf "%d " x) vsize;
       printf "\n");
    List.iter
      (fun x ->
         let b = ref Weight.zero 
         and unindexed = ref [] in    
           if debug then printf "processing doc %d\n" x.docid; 
           Docmatch_Vec.extend !o 
             (find_matches_1_remscore_minsize x maxweight_dim order vp vsize i t);
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if debug then (Printf.printf "b=%f\n" (Weight.to_float !b));
                if !b >= t then
                  (
                    if debug then printf "adding (%d, %f)\n" x.docid
                      (Weight.to_float dvelt.freq);
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                  )
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed;       
             ) x.vector;
           vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
           if debug then
             (Printf.printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nvp.(%d) = \n" x.docid; MyDV.print vp.(x.docid))
      ) v;
    !o (* return output set*)


(* split an *ordered* dv according to given array of term frequencies *)
let split_dv dim_nz sigma dv = 
  let len = Array.length dv.vector 
  and flag = ref false and num_h = ref 0 in
    if debug then (printf "splitting doc "; MyDV.print dv);
    while not !flag do
      if !num_h < len then (
        let termfreq = dim_nz.(dv.vector.(!num_h).term) in
          if termfreq < sigma then (
            flag := true;
            decr num_h;
          );
          incr num_h 
      )
      else
        flag := true
    done;
    let dvh = MyDV.make_dv dv.docid (Array.sub dv.vector 0 !num_h) dv.cat
    and dvl = MyDV.make_dv dv.docid (Array.sub dv.vector !num_h 
                                       (len - !num_h)) dv.cat in
      (dvh, dvl)

let find_matches_0_array2_fast dv i a t =
  let m = Docmatch_Vec.make () in
  let aset = Dynarray.make_reserved 0 (Array.length a) in
    if debug then
      (printf "find_matches_0 dv = "; MyDV.print dv;);
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter (fun (y, w)-> 
			if a.(y)=Weight.zero then
			  Dynarray.append aset y;
			a.(y) <- a.(y) +: (xw *: w)) i.(dvelt.term); 
      ) dv.vector;
    Dynarray.iter 
      (fun y ->
         let w = a.(y) in (
               if debug then 
                 printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
             if w >= t then 
               Docmatch_Vec.add (dv.docid, y, w) m;
	   );
           a.(y) <- Weight.zero  (* ready a for the next use *)
      ) aset; 
    m
      
(* v = list of document vectors
   t = threshold *)
let all_pairs_cutoff v t sigma = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v  (* number of dimensions in data set *)
  and n = List.length v in
  let a = Array.make n Weight.zero in
    (*and v = Util.map MyDV.normalize v in *)
  let maxweight_dim = maxweight_dim_preprocess v m 
  (* inverted lists *)
  and i = Array.init m (fun ix->MySV.make ())
  and dim_nz = calc_dim_nz v m 
  and order = reorder_dims v m in
  let vsplit = Util.map (split_dv dim_nz sigma) v
  (*let vh,vl = Util.split vsplit *)
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and t0 = ref Weight.zero
  and workh = ref 0 and workl = ref 0 and n_freq_term = ref 0
  and datah = ref 0 and datal = ref 0 and num_cand = ref 0 in
  let avg_t0 = ref 0.0 in 
    printf "sigma = %d\n" sigma;
    Array.iteri (fun i f -> 
                   if debug then (printf "dim %d, freq %d\n" i f);
                   if f>sigma then (
                     t0 := !t0 +: (maxweight_dim.(i) *: maxweight_dim.(i));
                     workh := !workh + f*f;
                     datah := !datah + f;
                     incr n_freq_term;
                     (*printf "freq term=%d, maxwgti=%f \n"
                       f (Weight.to_float maxweight_dim.(i))*)
                   )
                   else (
                     workl := !workl + f*f; 
                     datal := !datal + f))
      dim_nz; (*TODO: iterate over only frequent dimensions*)
    printf "n=%d, m=%d\n" n m; 
    printf "# freq terms = %d, t0=%f\n" !n_freq_term (Weight.to_float !t0);
    printf "workh=%d workl=%d\n" !workh !workl;
    printf "datah=%d datal=%d\n" !datah !datal; flush stdout;
    List.iter
      (fun (xh, xl) ->
         let id = xh.docid and t0 = ref Weight.zero in  
           if debug then 
             (printf "processing doc %d\n" id;
              printf "xh="; MyDV.print xh;
              printf "xl="; MyDV.print xl);
           Array.iter 
             (fun dvelt->
                t0:=!t0 +: maxweight_dim.(dvelt.term) *: dvelt.freq;
             )
             xh.vector;
           if debug then printf "t0=%f\n" (Weight.to_float !t0);
           let cand = find_matches_0_array2_fast xl i a (t -: !t0) in
           if profile then (
             num_cand := !num_cand + (Docmatch_Vec.cardinal cand);
             avg_t0 := !avg_t0 +. (Weight.to_float !t0);
           );
           Array.iter
             (fun dvelt->
                if debug then printf "checking term %d\n" dvelt.term;
                if debug then printf "adding (%d, %f)\n" id 
                     (Weight.to_float dvelt.freq);
                MySV.append i.(dvelt.term) (id, dvelt.freq)       
             ) xl.vector;
           v'.(id) <- xh;
           if debug2 then 
             (printf "\ni = \n"; 
              Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                i;
              Printf.printf "\nv'.(%d) = \n" id; MyDV.print v'.(id);
             );
           Docmatch_Vec.iter 
             (fun (x,y,s) -> 
                let w = s +: (MyDV.dot v'.(x) v'.(y) order) in
                  if w >= t then
                    Docmatch_Vec.add (x, y, w) !o
             ) cand;
      ) vsplit;
    if profile then (
      printf "total # cand=%d\n" !num_cand;
      avg_t0 := !avg_t0 /. (float_of_int (List.length vsplit));
      printf "avg t0=%g\n" !avg_t0;
    );
    !o (* return output set*)


(* v = list of document vectors
   t = threshold *)
let all_pairs_cutoff2 v t th = 
  let th = Weight.of_float th in
  let t = Weight.of_float t in
  let o = ref (Docmatch_Vec.make ()) in
  let m = 1 + max_dim v  (* number of dimensions in data set *)
  and n = List.length v in
    (*and v = Util.map MyDV.normalize v in *)
  let maxweight_dim = maxweight_dim_preprocess v m 
  (* inverted lists *)
  and i = Array.init m (fun ix->MySV.make ())
  and order = reorder_dims v m  
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in
  let num_cand = ref 0 in 
    List.iter
      (fun x ->
         let id = x.docid and t0 = ref Weight.zero in    
           if debug then  (printf "processing doc %d\n" id;
                           printf "x="; MyDV.print x;);
           let len = Array.length x.vector 
           and flag = ref false and num_h = ref 0 in
             while not !flag do
               if debug then 
                 (printf "ix=%d, t0=%g\n" !num_h (Weight.to_float !t0));
               if !num_h < len then (
                 let dvelt = x.vector.(!num_h) in
                   t0:=!t0 +: maxweight_dim.(dvelt.term) *: dvelt.freq; 
                   incr num_h;
                   if !t0 > th then (
                     flag := true;
                     decr num_h;
                     let dvelt = x.vector.(!num_h) in
                       t0:=!t0 -: maxweight_dim.(dvelt.term) *: dvelt.freq; 
                   )
               )
               else
                 flag := true
             done;
             let xh = MyDV.make_dv id (Array.sub x.vector 0 !num_h) x.cat
             and xl = MyDV.make_dv id (Array.sub x.vector !num_h 
                                         (len - !num_h)) x.cat in
             if debug then (printf "t0=%f ix=%d\n" (Weight.to_float !t0) !num_h;
                            printf "xh="; MyDV.print xh;
                            printf "xl="; MyDV.print xl
                           );
             let cand = find_matches_0 xl i (t -: !t0) in 
               num_cand := !num_cand + (Docmatch_Vec.cardinal cand);
               Array.iter (* index xl *)
                 (fun dvelt->
                    if debug then printf "checking term %d\n" dvelt.term;
                    if debug then printf "adding (%d, %f)\n" id 
                      (Weight.to_float dvelt.freq);
                    MySV.append i.(dvelt.term) (id, dvelt.freq)       
                 ) xl.vector;
               v'.(id) <- xh;
               if debug2 then 
                 (printf "\ni = \n"; 
                  Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x)
                    i;
                  Printf.printf "\nvp.(%d) = \n" id; MyDV.print v'.(id);
                 );
               Docmatch_Vec.iter 
                 (fun (x,y,s) -> 
                    let w = s +: (MyDV.dot v'.(x) v'.(y) order) in
                      if w >= t then
                        Docmatch_Vec.add (x, y, w) !o
                 ) cand;
      ) v;
    printf "total # candidates = %d\n" !num_cand;
    !o (* return output set*)
      
