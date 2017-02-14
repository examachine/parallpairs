(* 
**
** Row-wise parallelization of all-pairs similarity algorithms
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

open Types
open AllPairs
open Parallel
open ParAllPairsVert

module Weight = Types.Fix32Weight
module MyDV = AllPairs.MyDV
module MySV = AllPairs.MySV

open Weight 
open MyDV

let debug = false
let debug2 = false
let profile = false

let p = Mpi.comm_size Mpi.comm_world
let pid = Mpi.comm_rank Mpi.comm_world 

(* distribute a list of document vectors where part is a function  
   from docid to processor*)
let distribute_vectors v part =
  let myv = ref [] in
    if pid = 0 then (
      List.iter (
        fun dv ->
          let dest = part dv.docid in
            if dest = 0 then
              myv := dv :: !myv
            else (
              Mpi.send 1 dest dest Mpi.comm_world;
              Mpi.send dv dest dest Mpi.comm_world
            )
      ) v;
      for dest = 1 to p-1 do
        Mpi.send 0 dest dest Mpi.comm_world (* finish message *)
      done
    )
    else 
      while Mpi.receive 0 pid Mpi.comm_world = 1 do
        myv := (Mpi.receive 0 pid Mpi.comm_world)::!myv
      done;
    !myv

(* distribute a list of document vectors in cyclic order *)
let distribute_vecs_cyclic v t =
  distribute_vectors v (fun docid->docid mod p)

(* row parallelization of all pairs 0
   each processor is given a set of vectors
   first they process their local set of docs
   then every processor broadcasts their vectors to all procs one by one,
   and they are matched using the local inverted index built on those
   processors, quite close to the parallelization of knn algorithm 
   published in 2006.
*)
      
(* v = list of document vectors
   t = threshold *)
let par_all_pairs_0_horiz v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
    if debug then 
      (lprintf "\ni=\n";
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    let maxlocaln = Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
    in
    let v = Util.append v (Util.repeat 
                             {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                             (maxlocaln - List.length v)) in
      List.iter
        (fun x ->
           let xa = Mpi.allgather x Mpi.comm_world in 
             for proc=0 to p-1 do
               Docmatch_Vec.extend o 
                 (find_matches_0 xa.(proc) i t);
               if proc = pid then
	         Array.iter  (* construct local inverted index *)
                   (fun dvelt->
		      MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                   ) x.vector
             done;
        ) v;
      o (* return output set *)

let find_matches_0_array dv i a t =
  let m = Docmatch_Vec.make () in
  let n = Array.length a in
    if debug then
      (printf "find_matches_0 dv = "; MyDV.print dv;);
    Array.fill a 0 n Weight.zero;
    Array.iter
      (fun dvelt->
         let xw = dvelt.freq in
           MySV.iter 
             (fun (y, w)-> a.(y) <- a.(y) +: (xw *: w)) i.(dvelt.term); 
      ) dv.vector;
    for y = 0 to n - 1 do
      let w = a.(y) in (
          if debug then printf "%d %d w=%f\n" dv.docid y (Weight.to_float w);
          if w >= t then 
            Docmatch_Vec.add (dv.docid, y, w) m;
          if profile && w > Weight.zero then incr num_candidates;
	)
    done;
    m

(* v = list of document vectors
   t = threshold *)
let par_all_pairs_0_array_horiz v t = 
  let t = Weight.of_float t in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
  let myn = List.length v (* local number of docs *) in
  let n = Mpi.allreduce_int myn Mpi.Int_sum Mpi.comm_world in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = Docmatch_Vec.make_n (n*logn) in
  let maxlocaln = Mpi.allreduce_int myn Mpi.Int_max Mpi.comm_world in
  let v = Util.append v (Util.repeat 
                           {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                           (maxlocaln - myn)) in
  let a = Array.make n Weight.zero in
    List.iter
      (fun x ->
         if pid=0 && x.docid mod 100=0 then 
           (printf "doc %d     \r" x.docid; flush stdout);
         let xa = Mpi.allgather x Mpi.comm_world in 
           for proc=0 to p-1 do
             Docmatch_Vec.extend o 
               (find_matches_0_array xa.(proc) i a t);
             if proc = pid then
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
		    MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                 ) x.vector
           done;
      ) v;
    o (* return output set *)

(* v = list of document vectors
   t = threshold *)
let par_all_pairs_0_array_horiz_rownet v t partfile = 
  let t = Weight.of_float t in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
  let myn = List.length v (* local number of docs *) in
  let n = Mpi.allreduce_int myn Mpi.Int_sum Mpi.comm_world in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = Docmatch_Vec.make_n (n*logn) in
  let maxlocaln = Mpi.allreduce_int myn Mpi.Int_max Mpi.comm_world in
  let v = Util.append v (Util.repeat 
                           {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                           (maxlocaln - myn)) in
  let a = Array.make n Weight.zero in
    List.iter
      (fun x ->
         if pid=0 && x.docid mod 100=0 then 
           (printf "doc %d     \r" x.docid; flush stdout);
         let xa = Mpi.allgather x Mpi.comm_world in 
           for proc=0 to p-1 do
             Docmatch_Vec.extend o 
               (find_matches_0_array xa.(proc) i a t);
             if proc = pid then
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
		    MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                 ) x.vector
           done;
      ) v;
    o (* return output set *)

(* reorder dimensions in place assuming horizontal distribution *)
let par_reorder_dims_horiz v m = 
  let dim_nz_local = Array.make m 0 and dim_nz = Array.make m 0 in
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              dim_nz_local.(dvelt.term) <- dim_nz_local.(dvelt.term) + 1 
           ) x.vector
      ) v;
    Mpi.allreduce_int_array dim_nz_local dim_nz Mpi.Int_sum Mpi.comm_world; 
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


(* v = list of document vectors
   t = threshold *)
let par_all_pairs_1_horiz v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Set.make ()) in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world
  and n = Mpi.allreduce_int (List.length v) Mpi.Int_sum Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
    if debug then 
      (lprintf "\ni=\n";
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    let maxlocaln = Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
    in
    let order = par_reorder_dims_horiz v m
    and vp = Array.init n (fun ix->MyDV.make_emptydv ())  
    and v = Util.append v (Util.repeat 
                             {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                             (maxlocaln - List.length v))
    in
      List.iter
        (fun x ->
           let xa = Mpi.allgather x Mpi.comm_world in 
             for proc=0 to p-1 do
               o := Docmatch_Set.union !o 
                   (find_matches_1 xa.(proc) order vp i t);
             done;
             let b = ref Weight.zero
             and unindexed = ref [] in
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
                    b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                    if !b >=: t then
		      MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                    else
                      unindexed := (dvelt.term, dvelt.freq) :: !unindexed;
                 ) x.vector;
               vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
        ) v;
      !o (* return output set*)


(* just the minsize optimization *)
let par_horiz_find_matches_minsize dv maxweight_dim order vsize i t =
  (* TODO: estimate table size *)
  let a = Hashtbl.create 10000
  and m = ref (Docmatch_Set.make ()) in
  let minsizeloc = 
    (let mw = max_weight_dv dv in
       if mw = Weight.zero then
         10000.0 
       else
         Weight.to_float (t/:mw)
    )
  in
  let minsize = Weight.of_float
    (Mpi.allreduce_float minsizeloc Mpi.Float_min Mpi.comm_world)  in
    if debug then lprintf "par-all-pairs-minsize\n";
    if debug then
      (printf "find_matches_minsize %d, length=%f " dv.docid 
         (Weight.to_float (MyDV.magnitude dv));
       printf "dv = "; MyDV.print dv;
       printf "minsize=%f, max_weight_dv=%f\n" 
         (Weight.to_float minsize) (Weight.to_float (max_weight_dv dv)));
    Array.iter
      (fun dvelt->
         let t = dvelt.term and tw = dvelt.freq in
           while MySV.length i.(t) > 0 && 
            (Weight.of_float (float_of_int vsize.(fst (MySV.front i.(t) ))))
	     <: minsize (*+: epsilon_float*) do
               MySV.pop i.(t)
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
         if debug then Printf.printf "s(%d,%d)=%f\n" 
	   dv.docid y (Weight.to_float w); 
         if  w >=: t then
           m := Docmatch_Set.add (dv.docid, y, w) !m;
      )
      a;
    !m
  

(* v = list of document vectors
   t = threshold *)
let par_all_pairs_minsize_horiz v t = 
  let start_time = Sys.time () in
  let t = Weight.of_float t in
  let o = ref (Docmatch_Set.make ()) in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world
  and n = Mpi.allreduce_int (List.length v) Mpi.Int_sum Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
  let maxweight_dim : Weight.t array = max_weight_array (maxweight_dim_preprocess v m) 
   in 
  let vsizelocal = Array.make n 0
  and vsize = Array.make n 0 
  and max_weight = Array.make n 0.0 in
    if debug then lprintf "par-all-pairs-minsize_horiz\n";
    List.iter (fun x->
                 vsizelocal.(x.docid) <- Array.length x.vector;
                 max_weight.(x.docid) <- Weight.to_float (max_weight_dv x)
              ) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world; 
    if debug then 
      (lprintf "\ni=\n";
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    let maxlocaln = Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
    in
    let order = par_reorder_dims_horiz v m in
    (*and vp = Array.init n (fun ix->MyDV.make_emptydv ()) in *)
    let v = List.sort (fun a b -> Pervasives.compare 
                         (max_weight.(b.docid)) (max_weight.(a.docid))) v in
    let v = Util.append v (Util.repeat 
                             {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                             (maxlocaln - List.length v))
    in
    let init_time = (Sys.time () -. start_time) in
      if pid = 0 then
        printf "init time = %f\n" init_time;
      List.iter
        (fun x ->
           let xa = Mpi.allgather x Mpi.comm_world in 
             for proc=0 to p-1 do
               o := Docmatch_Set.union !o 
                 (par_horiz_find_matches_minsize
                      xa.(proc) maxweight_dim order vsize i t)
             done;
             (*let b = ref Weight.zero
             and unindexed = ref [] in*)
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
                    (*b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                    if !b >=: t then*)
		      MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                    (*else
                      unindexed := (dvelt.term, dvelt.freq) :: !unindexed;*)
                 ) x.vector;
              (* vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;*)
        ) v;
      !o (* return output set*)

(* just the minsize optimization *)
let par_horiz_find_matches_1_minsize dv maxweight_dim order vp vsize i t =
  let x = dv.docid 
  and tf = Weight.to_float t 
  and a = Hashtbl.create ((Array.length vp)/4)
  and m = ref (Docmatch_Set.make ()) in
  let minsizeloc = 
    (let mw = max_weight_dv dv in
       if mw = Weight.zero then
         10000.0 
       else
         tf /. (Weight.to_float mw)
    ) in
  let minsize = Mpi.allreduce_float minsizeloc Mpi.Float_min Mpi.comm_world in
    if debug then
      (printf "par_horiz_find_matches_1_minsize %d, length=%f " dv.docid 
         (Weight.to_float (MyDV.magnitude dv));
       printf "dv = "; MyDV.print dv;
       printf "minsize=%f, max_weight_dv=%f\n" 
         minsize (Weight.to_float (max_weight_dv dv)));
    Array.iter
      (fun dvelt->
         let ti = dvelt.term and xw = dvelt.freq in
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize  do
               MySV.pop i.(ti)
           done;
           MySV.iter 
             (fun (y, yw)->
                if debug2 then printf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                else
                  Hashtbl.add a y (xw *: yw);
             )
             i.(ti);
      ) dv.vector;
    if debug2 then 
      (printf "\npruned i=\n";
       Array.iteri (fun n x -> Printf.printf "%d: " n; MySV.print x) i);
    Hashtbl.iter
      (fun y sim -> 
         if debug2 then printf "y=%d, sim=%f\n" y (Weight.to_float sim);
         let s = sim +: (MyDV.dot dv vp.(y) order) in 
           if debug then Printf.printf "Sim of %d and %d = %f\n" x y 
             (Weight.to_float s); 
           if  s >= t then
             m := Docmatch_Set.add (x, y, s) !m;
      ) a;
    !m
  

(* v = list of document vectors
   t = threshold *)
let par_all_pairs_1_minsize_horiz v t = 
  let start_time = Sys.time () in
  let t = Weight.of_float t in
  let o = ref (Docmatch_Set.make ()) in
  (* maximum dimension in data set *)
  let m =  Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world
  and n = Mpi.allreduce_int (List.length v) Mpi.Int_sum Mpi.comm_world in
  (* inverted lists *)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
  let vsizelocal = Array.make n 0
  and vsize = Array.make n 0 
  and max_weight = Array.make n 0.0 in
    if debug then lprintf "par-all-pairs-minsize-horiz\n";
    List.iter (fun x->
                 vsizelocal.(x.docid) <- Array.length x.vector;
                 max_weight.(x.docid) <- Weight.to_float (max_weight_dv x)
              ) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world; 
    if debug then 
      (lprintf "\ni=\n";
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    let maxlocaln = Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
    in
    let order = par_reorder_dims_horiz v m in
    (*and vp = Array.init n (fun ix->MyDV.make_emptydv ()) in *)
    let v = List.sort (fun a b -> Pervasives.compare 
                         (max_weight.(b.docid)) (max_weight.(a.docid))) v in
    let v = Util.append v (Util.repeat 
                             {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                             (maxlocaln - List.length v)) in
    let vp = Array.init n (fun ix->MyDV.make_emptydv ()) in 
    let init_time = (Sys.time () -. start_time) in
      if pid = 0 then
        printf "init time = %f\n" init_time;
      if debug then 
        (lprintf "\ni=\n";
         Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
      List.iter
        (fun x ->
           let xa = Mpi.allgather x Mpi.comm_world in 
             for proc=0 to p-1 do
               o := Docmatch_Set.union !o 
                 (par_horiz_find_matches_1_minsize
                    xa.(proc) maxweight_dim order vp vsize i t)
             done;
             let b = ref Weight.zero
             and unindexed = ref [] in
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
                    b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                    if !b >=: t then
		      MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                    else
                      unindexed := (dvelt.term, dvelt.freq) :: !unindexed
                 ) x.vector;
               vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
        ) v;
      !o (* return output set*)
          

(* row parallelization of all pairs 0
   each processor is given a set of vectors
   first they process their local set of docs
   then every processor broadcasts their vectors to all procs one by one,
   and they are matched using the local inverted index built on those
   processors, quite close to the parallelization of knn algorithm 
   published in 2006.
*)

(* v = list of document vectors
   t = threshold *)
let all_pairs_bf_horiz v t = 
  let t = Weight.of_float t in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = Docmatch_ISet.make_n (n*logn) in
  (* inverted lists *)
  let maxlocaln = Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
  in
  let v = Util.append v (Util.repeat 
                           {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                           (maxlocaln - List.length v)) in
  let va = Array.of_list v in
    Array.iter MyDV.sort va;
    Array.iteri
      (fun i x ->
         if debug then (lprintf "v = " ; MyDV.lprint x);
         let xa = Mpi.allgather x Mpi.comm_world in 
           for proc=0 to p-1 do
             (* considering a vector received from proc 
                who should process it ?*)
             let xp = xa.(proc) in
               for j = 0 to i-1 do
                 let y = va.(j) in 
                 let w = MyDV.dot_ordered xp y in
                   if w >= t then
                     Docmatch_ISet.add (xp.docid,y.docid,w) o  
               done
           done;
      ) va;
    o (* return output set*)
