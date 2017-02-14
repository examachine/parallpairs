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
open Util

open Parallel
open Score


(* A naive parallelization of the algorithm to
   merge association lists with numerical value 
   al is an association list of (key,weight) pairs on each processor
*)


let par_merge_assoc_lists al =
  let al_list = Array.to_list (Mpi.allgather al Mpi.comm_world) in
    merge_assoc_lists al_list


let hypercube_accumulate_scores a  =
  let comm = ref Mpi.comm_world in 
  let par_partition a cube = 
    let color = if pid land (1 lsl cube)=0 then 0 else 1 in
    let nprocs = Mpi.comm_size !comm
    and id = Mpi.comm_rank !comm in
      if debug then 
        lprintf "in subcube: nprocs=%d, id=%d, color=%d\n" nprocs id color;
      let xnz,xlocal = 
        if a!=[] then
          1, choose_random (Array.of_list (List.map fst a))
        else
          0, 0 in
      let myarray = Array.of_list [xnz; xlocal] in
      let sumarray = Array.make 2 0 in
        Mpi.allreduce_int_array myarray sumarray Mpi.Int_sum !comm;
        let nzprocs,xsum = sumarray.(0), sumarray.(1) in
        let x = if nzprocs=0 then 0 else xsum/nzprocs in   
          if debug then lprintf "pivot=%d\n" x;
          comm := Mpi.comm_split !comm color pid;
          partition (fun elt->fst elt<=x) a
  in
  let d = Util.log2_int p in
  let b = ref a in (* accumulation buffer *)
    if debug then lprintf "d=%d\n" d;
    for i=d-1 downto 0 do (* process dimensions of hypercube *)
      let link = 1 lsl i in
      let partner = pid lxor link in
        if debug then lprintf "i=%d, partner=%d\n" i partner;
        if debug then lprint_assoc_list !b "b";
        let b0,b1 = par_partition !b i in
        if debug then lprintf "b partitioned\n"; 
        let tokeep = (if pid land link=0 then b0 else b1) 
        and tosend = (if pid land link=0 then b1 else b0) in
        let received = exchange tosend pid partner in 
          if debug then  
            (lprint_assoc_list tokeep "tokeep";
             lprint_assoc_list tosend "tosend";
             lprint_assoc_list received "received");
          b :=  merge_assoc_lists [tokeep; received]
    done;
    if debug then lprintf "finish score accum\n";
    !b


let hypercube_accumulate_scores_range a (kstart,kend) =
  let comm = ref Mpi.comm_world in 
  let par_partition a cube (kstart,kend) = 
    let color = if pid land (1 lsl cube)=0 then 0 else 1 in
    let nprocs = Mpi.comm_size !comm
    and id = Mpi.comm_rank !comm in
      if debug then 
        lprintf "in subcube: nprocs=%d, id=%d, color=%d\n" nprocs id color;
      let x = (kstart + kend /2) in
      let newrange = if color = 0 then (kstart, x) else (x+1, kend) in
        if debug then lprintf "pivot=%d\n" x;
        comm := Mpi.comm_split !comm color pid;
        ( (partition (fun elt->fst elt<=x) a), newrange) 
  in
  let d = Util.log2_int p in
  let b = ref a in (* accumulation buffer *)
  let range = ref (kstart, kend) in
    if debug then lprintf "d=%d\n" d;
    for i=d-1 downto 0 do (* process dimensions of hypercube *)
      let link = 1 lsl i in
      let partner = pid lxor link in
        if debug then lprintf "i=%d, partner=%d\n" i partner;
        if debug then lprint_assoc_list !b "b";
        let ((b0,b1), newrange) = par_partition !b i !range in
        if debug then lprintf "b partitioned\n"; 
        let tokeep = (if pid land link=0 then b0 else b1) 
        and tosend = (if pid land link=0 then b1 else b0) in
        let received = exchange tosend pid partner in 
          range := newrange;
          if debug then  
            (lprint_assoc_list tokeep "tokeep";
             lprint_assoc_list tosend "tosend";
             lprint_assoc_list received "received");
          b :=  merge_als tokeep received
    done;
    if debug then lprintf "finish score accum\n";
    !b

let hypercube_accumulate_scores_new a =
  let keys, values = List.split a in
  let maxk = 
    Mpi.allreduce_int (Util.max_list keys 0) Mpi.Int_max Mpi.comm_world in 
    hypercube_accumulate_scores_range a (0, maxk)

let hypercube_accumulate_scores_range_multi a (kstart,kend) =
  let comm = ref Mpi.comm_world in 
  let par_partition xs cube (kstart,kend) = 
    let color = if pid land (1 lsl cube)=0 then 0 else 1 in
    let nprocs = Mpi.comm_size !comm
    and id = Mpi.comm_rank !comm in
      if debug then 
        lprintf "in subcube: nprocs=%d, id=%d, color=%d\n" nprocs id color;
      let x = (kstart + kend /2) in
      let newrange = if color = 0 then (kstart, x) else (x+1, kend) in
        if debug then lprintf "pivot=%d\n" x;
        comm := Mpi.comm_split !comm color pid;
        ( Util.split 
            (Util.map (fun a->partition (fun elt->fst elt<=x) a) xs), 
         newrange) 
  in
  let d = Util.log2_int p in
  let b = ref a in (* accumulation buffer *)
  let range = ref (kstart, kend) in
    if debug then lprintf "d=%d\n" d;
    for i=d-1 downto 0 do (* process dimensions of hypercube *)
      let link = 1 lsl i in
      let partner = pid lxor link in
      let ((b0,b1), newrange) = par_partition !b i !range in
      let tokeep = (if pid land link=0 then b0 else b1) 
      and tosend = (if pid land link=0 then b1 else b0) in
      let received = exchange tosend pid partner in 
        range := newrange;
        b := Util.map2 merge_als tokeep received
    done;
    if debug then lprintf "finish score accum\n";
    !b


let hypercube_accumulate_scores_new_multi al_list =
  let maxkeys = 
    Util.map (fun a-> Util.max_list (fst (List.split a)) 0) al_list in 
  let maxk =  Mpi.allreduce_int (Util.max_list maxkeys 0) 
    Mpi.Int_max Mpi.comm_world in 
    hypercube_accumulate_scores_range_multi al_list (0, maxk)


let fold_assoc_lists a  =
  let d = Util.log2_int p in
  let partial = ref a in
    if debug then lprintf "d=%d\n" d;
    for dim=d-1 downto 0 do (* traverse dimensions of the hypercube *)
      let mask = (1 lsl dim) in
      let partner = pid lxor mask in
        if debug then lprintf "partner=%d\n" partner;
        if debug then lprint_assoc_list !partial "partial";
        let a0,a1 = List.partition (fun x->(fst x mod p) land mask=0)
          !partial in
        let tokeep = (if pid land mask=0 then a0 else a1) 
        and tosend = (if pid land mask=0 then a1 else a0) in
        let received = exchange tosend pid partner in 
        if debug then 
          (lprint_assoc_list tokeep "tokeep";
           lprint_assoc_list tosend "tosend";
           lprint_assoc_list received "received");
          partial := merge_assoc_lists [tokeep; received]; 
    done;
    !partial
  
      

(* faster parallelization of the assocation list merging algorithm
   each key is given a home processor (key mod p)
   we make p collective communications, in each of which we 
   reduce to processor p pairs with the key assigned to it. The
   reduction is performed by multi node accumulation to all in which
   the combination operator merges two association lists into one.
   
*)
let hypercube_accumulate_scores_fast al =
  let alslice = Array.make p [] in
    (*partition pairs according to cyclic-distribution of key *)
    List.iter
      (fun (key,weight)->
         let dest = key mod p in
           alslice.(dest) <- (key,weight)::alslice.(dest)
      ) al;
    let alslice = Array.map List.rev alslice in 
    let result = ref [] in
      for dest=0 to p-1 do
        let x = hypercube_mnac_all alslice.(dest) merge_als in
          if dest=pid then
            result := x
      done;
      !result


(* use Mpi.gather instead of mnac_all *)
let accumulate_scores_fast2 ?(comm=Mpi.comm_world) al =
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslice = Array.make p [] in
    (*partition pairs according to cyclic-distribution of key *)
    List.iter
      (fun (key,weight)->
         let dest = key mod p in
           alslice.(dest) <- (key,weight)::alslice.(dest)
      ) al;
    let alslice = Array.map List.rev alslice in 
    let result = ref [] in
      for dest=0 to p-1 do
        let xa = Mpi.gather alslice.(dest) dest comm in 
          if dest=pid then
            result :=  Array.fold_left merge_als [] xa 
      done;
      !result

let accumulate_scores_fast2_multi ?(comm=Mpi.comm_world) al_list 
    : (int * Weight.t) list list =
  let bsize = List.length al_list in
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslices = Array.init p (fun x->Array.make bsize []) in
    list_iteri 
      (fun ix al ->
         if debug then (lprintf "al_list %d " ix;
                        lprint_assoc_list al "al");
         (*partition pairs according to cyclic-distribution of key *)
         List.iter
           (fun (key,weight)->
              let dest = key mod p in
                alslices.(dest).(ix) <- (key,weight)::alslices.(dest).(ix)
           ) (List.rev al);
      ) al_list;
    let t0 = new timer in
    let my_xas = ref alslices in
      for dest=0 to p-1 do
        let xas = Mpi.gather alslices.(dest) dest comm in
          if dest=pid then
            my_xas := xas
      done;
      lprintf "accumulate mpi.gather time=%g\n" t0#elapsed;
      let t1 = new timer in
      let al_arr = Array.fold_left
        (fun al_arr1 al_arr2->
           array_map2 merge_als al_arr1 al_arr2)
        (Array.make bsize []) !my_xas in
      let results = Array.to_list al_arr in
        lprintf "accumulate fold time=%g\n" t1#elapsed;
        if debug then 
          List.iter (fun x->lprint_assoc_list x "results") results;
        results

(* does not assume they are sorted *)
let hypercube_par_merge_assoc_lists al =
  hypercube_mnac_all al (fun x y->merge_assoc_lists [x;y]) 

(* does not seem to work right *)
let hypercube_par_merge_sorted_assoc_lists al =
  hypercube_mnac_all al merge_als

(* use aapc to reduce scores *)
let accumulate_scores_aapc ?(comm=Mpi.comm_world) al =
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslice = Array.make p [] in
    (* partition pairs according to cyclic-distribution of key *)
    List.iter
      (fun (key,weight)->
         let dest = key mod p in
           alslice.(dest) <- (key,weight)::alslice.(dest)
      ) al;
    let alslice = Array.map List.rev alslice in 
    let almine = Parallel.aapc alslice in
      Array.fold_left merge_als [] almine

let lprint_dynarray printelt l =
  lprintf "[[ ";
  Dynarray.iter (fun x-> printelt x; lprintf "; ") l;
  lprintf " ]]"

let lprint_intdynarray l = lprint_dynarray lprint_int l

let lprint_floatdynarray l = lprint_dynarray lprint_float l

let lprint_wgtdynarray l = lprint_dynarray (fun x->lprint_float 
                                              (Weight.to_float x)) l

(* use aapc to reduce scores given a pair of id and weight arrays *)
let accumulate_scores_pair_aapc ?(comm=Mpi.comm_world) (alv,alw) =
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslice = Array.init p 
    (fun i-> DynarrayPair.init (fun i->0) (fun i->Weight.zero)) in
    (* partition pairs according to cyclic-distribution of key *)
    DynarrayPair.iter 
      (fun u w ->
         let dest = u mod p in
           DynarrayPair.append alslice.(dest) u w
      ) (alv, alw);  
    let almine = Parallel.aapc ~comm:comm alslice in
      Array.fold_left merge_dynarray_pair_sets 
        (DynarrayPair.make 0 Weight.zero) almine


(* reduce scores given a list of pairs of id and weight arrays *)
let accumulate_scores_pair_multi ?(comm=Mpi.comm_world) al_list =
  let bsize = List.length al_list in
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslice = Array.init p 
    (fun i-> 
       Array.init bsize 
         (fun i->DynarrayPair.init (fun i->0) (fun i->Weight.zero))) in
    list_iteri
    (fun ix al ->
       (* partition pairs according to cyclic-distribution of key *)
       DynarrayPair.iter 
         (fun u w ->
            let dest = u mod p in
              DynarrayPair.append alslice.(dest).(ix) u w
         ) al;
    ) al_list;
    let almine = Parallel.aapc alslice in
      Array.fold_left 
        (fun al_arr1 al_arr2->
           array_map2 merge_dynarray_pair_sets al_arr1 al_arr2) 
        (Array.make bsize (DynarrayPair.init (fun i->0)
                             (fun i->Weight.zero))) almine
                      

let accumulate_scores_fast2_multi ?(comm=Mpi.comm_world) al_list 
    : (int * Weight.t) list list =
  let bsize = List.length al_list in
  let p = Mpi.comm_size comm in
  let pid = Mpi.comm_rank comm in
  let alslices = Array.init p (fun x->Array.make bsize []) in
    list_iteri 
      (fun ix al ->
         if debug then (lprintf "al_list %d " ix;
                        lprint_assoc_list al "al");
         (*partition pairs according to cyclic-distribution of key *)
         List.iter
           (fun (key,weight)->
              let dest = key mod p in
                alslices.(dest).(ix) <- (key,weight)::alslices.(dest).(ix)
           ) (List.rev al);
      ) al_list;
    let t0 = new timer in
    let my_xas = ref alslices in
      for dest=0 to p-1 do
        let xas = Mpi.gather alslices.(dest) dest comm in
          if dest=pid then
            my_xas := xas
      done;
      lprintf "accumulate mpi.gather time=%g\n" t0#elapsed;
      let t1 = new timer in
      let al_arr = Array.fold_left
        (fun al_arr1 al_arr2->
           array_map2 merge_als al_arr1 al_arr2)
        (Array.make bsize []) !my_xas in
      let results = Array.to_list al_arr in
        lprintf "accumulate fold time=%g\n" t1#elapsed;
        if debug then 
          List.iter (fun x->lprint_assoc_list x "results") results;
        results
