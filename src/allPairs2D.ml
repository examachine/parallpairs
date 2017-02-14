(* 
**
** 2D Parallelization of all-pairs algorithm
** Each processor is assigned a block of features and vectors
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
open Util

module Weight = Types.Fix32Weight
module MyDV = AllPairs.MyDV
module MySV = AllPairs.MySV

open Weight
open MyDV
 
let debug = false
let debug2 = false
let profile = true

(*
  two-dimensional checkerboard partitioning

  q = number of row blocks
  r = number of col blocks
  partvec id = row block
  partdim id = col block
  dest = (partdim dimid) + (partvec dvid) * r 

  distribute vectors and terms according to
  given partition vectors
  input on processor 0: document vectors, partition vectors
  output on processor i: part of document vectors 
  containing vectors and terms assigned to processor i
*)

let partition_checkerboard v q r partvec partdim =
  let mydvlist = ref [] in
    if pid = 0 then (
      List.iter (
        fun dv ->
	  if debug then (lprintf "processing"; MyDV.lprint dv; lprintf "\n");
          let rowblock = partvec dv.docid in
          let split_dv = Array.make r [] in
	    if debug then lprintf "rowblock=%d\n" rowblock;
            assert(0<=rowblock && rowblock<q);
            Array.iter
              (fun dvelt ->
                 let colblock = partdim dvelt.term in
		   if debug then lprintf "colblock=%d\n" colblock;
                   assert(0<=colblock && colblock<r);
                   split_dv.(colblock) <- dvelt :: split_dv.(colblock)
              ) dv.vector;
            let split_dv = Array.map List.rev split_dv in
              Array.iteri
                (fun colblock dv_vec ->
                   let dest = rowblock * r + colblock in
                   let dv_i = {dv with vector = Array.of_list dv_vec} in
                     (* send to destination processor *)
                     if dest = 0 then
                       mydvlist := dv_i :: !mydvlist
                     else (
                       Mpi.send 1 dest dest Mpi.comm_world;
                       Mpi.send dv_i dest dest Mpi.comm_world;
                     )
                ) split_dv;
          
      ) v;
      for dest = 1 to p-1 do
        Mpi.send 0 dest dest Mpi.comm_world (* finish message *)
      done
    )
    else 
      while Mpi.receive 0 pid Mpi.comm_world = 1 do
        mydvlist := (Mpi.receive 0 pid Mpi.comm_world)::!mydvlist
      done;
    (List.rev !mydvlist)
 
let distribute_2d_cyclic v q r =
  partition_checkerboard v q r (fun i->i mod q) (fun i->i mod r)

let distribute_2d_cyclic_scalar v q r =
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partitioning dimensions according to all-pairs-0 load\n";
      flush stdout;
      let dim_nz = calc_dim_nz v m in
      let dims = 
        Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = 
        List.sort (fun a b -> compare (snd b) (snd a)) dims in
 
        (* distribute terms to balance computational load *)        
        let pweight = Array.make r 0.0 in
        let minweightproc = ref 0 in
          List.iter
            (fun (dim, freq) -> 
               let wgt = float_of_int (freq * (freq - 1 ) / 2) in
               let proc = !minweightproc in
                 partition.(dim) <- proc;
                 pweight.(proc) <- pweight.(proc) +. wgt;
                 let mwgtix = ref 0 and minwgt = ref pweight.(0) in
                   Array.iteri (fun i w ->
                                  if w < !minwgt then (
                                    minwgt := w;
                                    mwgtix := i;
                                  )) pweight;
                   minweightproc := !mwgtix;
            ) sorted_dims ;
          lprintf "partition weight array: ";
          lprint_floatarray pweight;
	  if debug then
	    (lprintf "partition vector: "; 
	     lprint_intarray partition; lprintf "\n")
    );
    partition_checkerboard  v q r (fun i->i mod q) (fun i->partition.(i))

(* Todo: use cart_create *)
let make_2d_comms q r =
  let pid = Parallel.pid in
  let row = pid / r in
  let col = pid mod r in
  let row_comm = Mpi.comm_split Mpi.comm_world row pid 
  and col_comm = Mpi.comm_split Mpi.comm_world col pid in
    lprintf "row=%d, col=%d\n" row col;
    (row_comm, col_comm)

let docid_range v =
  if (v=[]) then
    (0,0)
  else
    let minid = ref (List.hd v).docid and maxid = ref 0 in
      List.iter (fun dv-> 
                   if dv.docid < !minid then minid := dv.docid;
                   if dv.docid < !maxid then maxid := dv.docid;) v;
      (!minid, !maxid)

let par_find_matches_0_vert_array ?(comm=Mpi.comm_world) dv n i t =
  let a = Array.make n Weight.zero in
  let o = Docmatch_Vec.make () in
  let x = dv.docid in
    if debug then
      (lprintf "par_find_matches_0_vert_array %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    Array.iter
      (fun dvelt->
         let xt = dvelt.term and xw = dvelt.freq in
           MySV.iter (fun (y, w)->  a.(y) <- a.(y) +: (xw *: w)) i.(xt); 
      ) dv.vector;
    if debug2 then
      (lprintf "\nlocal a =";
       Array.iter (fun x->lprintf "%f " (Weight.to_float x)) a);
    merge_scores_array ~comm:comm x a t o;
    o

(* v = list of document vectors
   t = threshold *)
let all_pairs_2d v t q r =  
  let t = Weight.of_float t in
  (* maximum dimension in data set *)
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let myrow, mycol = make_2d_comms q r in
  (*let vrange = docid_range v in*)
  let i = Array.init (m+1) (fun ix->MySV.make ()) in (* inverted lists *)
  let myn = List.length v (* local number of docs *) in
  let n = Mpi.allreduce_int myn Mpi.Int_sum mycol in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = Docmatch_Vec.make_n (n*logn) in
  let maxlocaln = 
    Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
  in
  let v = Util.append v (Util.repeat 
                           {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                           (maxlocaln - myn)) in
    List.iter
      (fun x ->
         if pid=0 && x.docid mod 100=0 then 
           (printf "doc %d     \r" x.docid; 
            (*Parallel.check_memory ();*) flush stdout);
         if debug then (lprintf "doc: "; MyDV.lprint x);
         let xa = Mpi.allgather x mycol in
         let colid = Mpi.comm_rank mycol in
           for proc=0 to q-1 do
             Docmatch_Vec.extend o
               (par_find_matches_0_vert_array ~comm:myrow xa.(proc) n i t);
             if proc = colid then
	       Array.iter  (* construct local inverted index *)
                 (fun dvelt->
	            MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                 ) x.vector;
           done;
      ) v;
    o (* return output set*)


(* v = list of document vectors
   t = threshold *)
let all_pairs_2d_opt v t q r =  
  let t = Weight.of_float t in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  (* m is the number of dimensions in the dataset *)
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let myrow, mycol = make_2d_comms q r in
  let i = Array.init (m+1) (fun ix->MySV.make ()) in (* inverted lists *)
  let myn = List.length v (* local number of docs *) in
  let n = Mpi.allreduce_int myn Mpi.Int_sum mycol in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = Docmatch_Vec.make_n (n*logn) in
  let maxlocaln = 
    Mpi.allreduce_int (List.length v) Mpi.Int_max Mpi.comm_world
  in
  let v = Util.append v (Util.repeat 
                           {(MyDV.make_emptydv ()) with MyDV.docid = -1 }
                           (maxlocaln - myn)) in
  let vblocks = partition_vectors_blocksize v 32 in 
    List.iter
      (fun vblock ->
         lprintf "processing vector block\n";
         if pid=0 then (printf "."; flush stdout);
         let vblocka = Mpi.allgather vblock mycol in
         let colid = Mpi.comm_rank mycol in
         let t0 = new timer in
         let candidates = ref [] in
           for proc=0 to q-1 do
               List.iter
		 (fun x ->
                    if debug then lprintf "processing doc %d\n" x.docid; 
                    let a,cl = compute_scores_matches_0_array x n i t_local 
                    in
                      candidates := (x.docid,a,cl) :: !candidates;
		      if proc = colid then
			Array.iter  (* construct local inverted index *)
			  (fun dvelt->
			     MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
			  ) x.vector;
		 ) vblocka.(proc);
               comp_time := !comp_time +. t0#elapsed;
           done;
           lprintf "compute scores %g sec\n" t0#elapsed;
           let t1 = new timer in
	     hypercube_merge_scores_multi ~comm:myrow !candidates t o;
	     lprintf "merge scores %g sec\n" t1#elapsed;
      ) vblocks;
    o (* return output set*)
