(*
  Dimension-wise parallelization of all-pairs algorithm
  Copyright 2010, Eray Ozkural <examachine@gmail.com>
*)

open Printf
open Types
open AllPairs
open Parallel
open Util
open Score
open ScoreAccum

module Weight = Types.Fix32Weight
module MyDV = AllPairs.MyDV
module MySV = AllPairs.MySV

open Weight
open MyDV
 
let debug0 = false
let debug = false
let debug2 = false
let profile = true

let blocksize = ref 64

let p = Mpi.comm_size Mpi.comm_world
let pid = Mpi.comm_rank Mpi.comm_world 
let num_candidates = ref 0
let num_cmap = ref 0
let num_scores = ref 0 (* total number of scores communicated *)
let my_scores = ref 0
let comm_time = ref 0.0
let comm_time2 = ref 0.0
let find_matches_time = ref 0.0
let barrier_time2 = ref 0.0
let tot_comm_time = ref 0.0
let comp_time = ref 0.0
let total_candidates = ref 0 

let block_dist_int_interval (a,b) =
  if pid = p-1 then
    (((b-a) / p) * (p-1) , b)
  else
    (((b-a) / p) * pid , ((b-a) / p) * (pid+1) - 1)

(*let which_dest id (a,b) =*)

let in_range x (a,b) = a <= x && x <= b

(*
  initial distribution

  cyclic distribution of features

  input on processor 0: a document vector, 
  output on processor i: portion of document vector

*)

let distribute_dims_cyclic_dv dv =
  let split_dv = Array.make p [] in
    Array.iter
      (fun dvelt ->
         let dest = dvelt.term mod p in
           split_dv.(dest) <- dvelt :: split_dv.(dest)
      ) dv.vector;
    let split_dv = Array.map List.rev split_dv in
      if pid = 0 then
        (for dest=1 to p-1 do
           let dv_i = 
             {dv with vector = Array.of_list split_dv.(dest)} in
             Mpi.send dv_i dest dest Mpi.comm_world
         done;
         {dv with vector = Array.of_list split_dv.(0)}
        )
      else
        Mpi.receive 0 pid Mpi.comm_world

let distribute_dims_cyclic_dvlist dvlist t =
  let len = Mpi.broadcast (List.length dvlist) 0 Mpi.comm_world in
    if pid = 0 then
      Util.map distribute_dims_cyclic_dv dvlist
    else
      Util.map distribute_dims_cyclic_dv (Util.repeat (make_emptydv ()) len)

(*
  distribute terms according to given partition vector
  input on processor 0: a document vector, partition vector
  output on processor i: a part of document vector 
  containing terms assigned to processor i
*)

let distribute_dims_dv dv partition =
  (*if pid = 0 then (printf "partitioning doc"; MyDV.print dv);*)
  let split_dv = Array.make p [] in
    Array.iter
      (fun dvelt ->
         let dest = partition.(dvelt.term) in
           assert (0<=dest && dest<p);
           split_dv.(dest) <- dvelt :: split_dv.(dest)
      ) dv.vector;
    let split_dv = Array.map List.rev split_dv in
      if pid = 0 then
        (for dest=1 to p-1 do
           let dv_i = 
             {dv with vector = Array.of_list split_dv.(dest)} in
             Mpi.send dv_i dest dest Mpi.comm_world
         done;
         {dv with vector = Array.of_list split_dv.(0)}
        )
      else
        Mpi.receive 0 pid Mpi.comm_world


let distribute_dims_decr_dens_cyclic_dvlist v t =
  let n = Mpi.broadcast (List.length v) 0 Mpi.comm_world in
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partition dimensions in cyclic decreasing density order\n";
      let dim_nz = calc_dim_nz v m in
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = List.sort (fun a b -> compare (snd b) (snd a)) dims in
          Util.list_iteri 
            (fun ix (dim, freq) -> partition.(dim) <- ix mod p) sorted_dims ;
    );
    let v = if pid = 0 then v else (Util.repeat (make_emptydv ()) n) in
    let partition = Mpi.broadcast partition 0 Mpi.comm_world in
      if debug then (lprintf "partition array: "; lprint_intarray partition);
      Util.map (fun dv->distribute_dims_dv dv partition) v

let distribute_dims_num_nz_dvlist v t =
  let n = Mpi.broadcast (List.length v) 0 Mpi.comm_world in
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partition dimensions to balance the number of non-zeroes\n";
      let dim_nz = calc_dim_nz v m in
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = List.sort (fun a b -> compare (snd b) (snd a)) dims in
 
        (* distribute terms to balance computational load *)        
        let pweight = Array.make p 0.0 in
        let minweightproc = ref 0 in
          List.iter
            (fun (dim, freq) -> 
               let wgt = float_of_int freq in
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
    );
    let v = if pid = 0 then v else (Util.repeat (make_emptydv ()) n) in
    let partition = Mpi.broadcast partition 0 Mpi.comm_world in
      if debug then (lprintf "partition array: "; lprint_intarray partition);
      Util.map (fun dv->distribute_dims_dv dv partition) v

let distribute_dims_allpairs0_dvlist v t =
  let n = Mpi.broadcast (List.length v) 0 Mpi.comm_world in
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partition dimensions according to all-pairs-0 load\n";
      let dim_nz = calc_dim_nz v m in
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = List.sort (fun a b -> compare (snd b) (snd a)) dims in
 
        (* distribute terms to balance computational load *)        
        let pweight = Array.make p 0.0 in
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
          lprint_floatarray pweight; lprintf "\n";
    );
    let v = if pid = 0 then v else (Util.repeat (make_emptydv ()) n) in
    let partition = Mpi.broadcast partition 0 Mpi.comm_world in
      if debug then (lprintf "partition array: "; lprint_intarray partition);
      Util.map (fun dv->distribute_dims_dv dv partition) v

(* all-pairs load for power-low datasets *) 
let distribute_dims_allpairs_powerlaw_dvlist v t =
  let n = Mpi.broadcast (List.length v) 0 Mpi.comm_world in
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partition dimensions according to all-pairs-0 power-law load\n";
      let dim_nz = calc_dim_nz v m in
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = List.sort (fun a b -> compare (snd b) (snd a)) dims in
 
        (* distribute terms to balance computational load *)        
        let pweight = Array.make p 0.0 in
        let minweightproc = ref 0 in
          List.iter
            (fun (dim, freq) -> 
               let freq = float_of_int freq in 
               let wgt = (freq *. (freq -. 1.0 ) /. 2.0) +. (if freq<(float_of_int (n/16)) then 0.0 else freq)**2.5 in
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
          lprint_floatarray pweight
    );
    let v = if pid = 0 then v else (Util.repeat (make_emptydv ()) n) in
    let partition = Mpi.broadcast partition 0 Mpi.comm_world in
      if debug then (lprintf "partition array: "; lprint_intarray partition);
      Util.map (fun dv->distribute_dims_dv dv partition) v

(* estimate all-pairs-1 load, estimate sparse execution time as
   choose(idx,2) mul ops where idx is the number of indexed number of nonzeroes in the nth dimension. freq-choose(idx,2) is the number of non-indexed number of nonzeroes and we must also take into account that each non-indexed nonzero is going to be multiplied with... the total number of candidates resulting from the indexed portion, which is, on the average n/4. that is, this model estimates the number of multiplication operatios during the running of all-pairs-1.
*)

let distribute_dims_allpairs1_dvlist v t =
  let n = Mpi.broadcast (List.length v) 0 Mpi.comm_world in
  let m = Mpi.broadcast (1 + max_dim v) 0 Mpi.comm_world in 
  let partition = Array.make m 0 in
    if pid = 0 then ( 
      printf "Partition dimensions according to all-pairs-1 load\n";
      let maxweight_dim = maxweight_dim_preprocess v m in
      let b = ref Weight.zero in
      let dim_idx = Array.make m 0 in (* number of indexed docs per term *) 
      let dim_nz = calc_dim_nz v m in
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz) in
      let sorted_dims = List.sort (fun a b -> compare (snd b) (snd a)) dims in
        (* calculate number of indexed dv elts per term *)
        List.iter 
          (fun dv -> 
             if debug then lprintf "distribute dv %d\n" dv.docid;
              Array.iter 
               (fun dvelt ->
                  b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                  if debug then printf "b=%f\n" (Weight.to_float !b);
                  if !b >=: t then
                    dim_idx.(dvelt.term) <- dim_idx.(dvelt.term) + 1
               ) dv.vector
          ) v;

        (* distribute terms to balance computational load *)        
        let pweight = Array.make p 0.0 in
        let pwidx = Array.make p 0.0 in 
        let pnidx = Array.make p 0 in 
        let minweightproc = ref 0 in
        let vsize = Array.make (List.length v) 0 in
          List.iter
            (fun x->vsize.(x.docid) <- Array.length x.vector) v;  
          List.iter
            (fun (dim, freq) ->
               let num_idx = dim_idx.(dim) in 
               let num_nidx = freq - num_idx in
               let wgt = float_of_int 
                 ((num_idx*(num_idx-1)/2) + (num_nidx*(num_nidx)/2)) in
               let proc = !minweightproc in
                 partition.(dim) <- proc;
                 pweight.(proc) <- pweight.(proc) +. wgt;
                 pwidx.(proc) <- pwidx.(proc) +. 
                   float_of_int (num_idx*(num_idx-1)/2);
                 pnidx.(proc) <- pnidx.(proc) + num_nidx;
                 let mwgtix = ref 0 and minwgt = ref pweight.(0) in
                   Array.iteri (fun i w ->
                                  if w < !minwgt then (
                                    minwgt := w;
                                    mwgtix := i;
                                  )) pweight;
                   minweightproc := !mwgtix;
            ) sorted_dims;
          lprintf "partition weight array: ";
          lprint_floatarray pweight;
          lprintf "partition indexed weight array: ";
          lprint_floatarray pwidx;
          lprintf "partition num num non-idx array: ";
          lprint_intarray pnidx; lprintf "\n"
    );
    let v = if pid = 0 then v else (Util.repeat (make_emptydv ()) n) in
    let partition = Mpi.broadcast partition 0 Mpi.comm_world in
      if debug then (lprintf "partition array: "; lprint_intarray partition);
      Util.map (fun dv->distribute_dims_dv dv partition) v

let partition_vectors_blocksize v block_size =
  let rec loop v vb =
    match v with 
        [] -> vb
      | vl -> 
          let firstn,rest = split_first_n block_size vl in
          loop rest (firstn::vb)
  in
    List.rev (loop v [])

let print_candidates a =
  Hashtbl.iter
    (fun y w -> lprintf "(%d, %f) " y (Weight.to_float w) ) a;
  lprintf "\n"

(*
  merge local a's across processors for 
  dimension-wise partitioning of vectors
*)
let hypercube_merge_scores_hashtbl x a t o  =       
  let t_local = t/:(Weight.of_float (float_of_int p)) in 
    (* TODO:  faster to distribute output *) 
  let cl = ref [] in (* local candidate list *)
    Hashtbl.iter
      (fun y w -> 
         if w > t_local then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
      let c_l = List.sort compare (List.rev !cl) in
      let t0 = new timer in
      (*let c_g = Parallel.reduce_all c_l Parallel.merge_sets [] in*)
      let c_g = Parallel.hypercube_mnac_all c_l Util.merge_sets in
      let al = ref [] in
        if debug then
          (lprintf "c_l =  "; lprint_intlist c_l;
           lprintf "c_g =  "; lprint_intlist c_g);
        if profile then 
          (num_candidates := !num_candidates + (List.length c_l);
           comm_time := !comm_time +. t0#elapsed);
        List.iter
          (fun y -> (* if weight>0 we have already calculated its local count*)
             if Hashtbl.mem a y then
               let w = Hashtbl.find a y in 
                  al := (y, w) :: !al (* add (y,w) as a local count *) 
          ) c_g;
        (*lprint_assoc_list !al "al";*)
        let al_sorted = 
          List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
        let t1 = new timer in
        let scores_mine =
          accumulate_scores_fast2 al_sorted
        in
          if debug then
            (lprint_assoc_list al_sorted "al_sorted";
             lprint_assoc_list scores_mine "scores_mine"; 
             lprintf "\n");
            if profile then 
            (num_scores:=!num_scores + (List.length !al);
             comm_time2:=!comm_time2+.t1#elapsed;
             (*lprint_assoc_list al_sorted "sorted";*)
            ); 
          List.iter
            (fun (y,s) -> 
               if debug2 then lprintf "Sim of %d and %d = %f\n" x y 
                 (Weight.to_float s); 
               if  s >= t then
                 Docmatch_ISet.add (x, y, s) o
            ) scores_mine

(* column (feature) block parallelization of all pairs 
   each processor holds a number of dimensions (features),
   weighed by the square of number of nonzeros (?)
   therefore each document is split across processors while
   each inverted list is stored wholesome.
*)

let par_find_matches_0_vert dv n i t =
  let x = dv.docid in 
  let a = Hashtbl.create (n/2)
  and m = ref (Docmatch_Set.make ())
  and num_scalar_products = ref 0 in
    if debug then
      (lprintf "find_matches_0_vert %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    Array.iter
      (fun dvelt->
         let (*t = dvelt.term and*) tw = dvelt.freq in
           MySV.iter 
             (fun (y, yw)->
                if debug2 then lprintf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  tw *: yw)
                else 
                  Hashtbl.add a y (tw *: yw);
                if profile then incr num_scalar_products;
             )
             i.(dvelt.term);
      ) dv.vector;

    if debug then (
      lprintf "\nlocal a = "; print_candidates a;
    );

    hypercube_merge_scores_hashtbl x a t !m;
    !m

(* feature-wise parallelization of all-pairs-0
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold  *)

let par_all_pairs_0_vert v t = 
  let t = Weight.of_float t in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let o = ref (Docmatch_Set.make_n (n*logn)) in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
    if debug then lprintf "par-all-pairs-0-vert\n";
    if profile then total_candidates := 0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun x ->
         if x.docid mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" x.docid; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         o := Docmatch_Set.union !o 
           (par_find_matches_0_vert x n i t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    !o (* return output set*)

(*
  column (feature) block parallelization of all-pairs-0-minsize
  each processor holds a number of dimensions (features),
  weighed by the square of number of nonzeros (?)
  therefore each document is split across processors while
  each inverted list is stored wholesome. an easy way to 
  achieve this is cyclic distribution of dimensions.
  
  since partial indexing would incur a lot of communication
  overhead, we only implement the minsize optimization, which
  requires an ordering of documents (rows)
*)

let par_find_matches_0_minsize dv maxweight_dim vsize i t =
  (* TODO: estimate table size *)
  let tf = Weight.to_float t in
  let x = dv.docid in
  let n = Array.length vsize in
  let a = Hashtbl.create (n/2)
  and m = ref (Docmatch_Set.make ())
  and maxweightdv = 
    (Weight.of_float (Mpi.allreduce_float 
                        (Weight.to_float (max_weight_dv dv)) 
                        Mpi.Float_max Mpi.comm_world) )
  in 
  if maxweightdv=Weight.zero then !m else
  let minsize = (tf /. (Weight.to_float (max_weight_dv dv))) in
    if debug then
      (lprintf "find_matches_0_minsize %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       if debug2 then (lprintf "dv = "; MyDV.lprint dv);
       lprintf "minsize=%f, max_weight_dv=%f\n" 
         minsize (Weight.to_float maxweightdv));
    Array.iter
      (fun dvelt->
         let ti = dvelt.term and xw = dvelt.freq in
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize  do
               MySV.pop i.(ti) 
           done; 
           MySV.iter 
             (fun (y, yw)->
                if debug2 then lprintf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                else
                  Hashtbl.add a y (xw *: yw);
             )
             i.(dvelt.term);
      ) dv.vector;
    if debug2 then 
      (lprintf "\npruned i=\n";
       Array.iteri (fun n x -> lprintf "%d: " n; 
                      MySV.lprint x) i);
    hypercube_merge_scores_hashtbl x a t !m;
    !m

(* parallel computation of the maximum of an array of weights *)
let max_weight_array a =
  let len = Array.length a in
  let a_float_local = Array.map Weight.to_float a in
  let a_float = Array.make len 0.0 in
  let a_global = Array.make len Weight.zero in
    Mpi.allreduce_float_array a_float_local a_float
      Mpi.Float_max Mpi.comm_world;
    (*TODO: use map*)
    Array.iteri (fun i x->a_global.(i) <- Weight.of_float x) a_float; 
    a_global
  
(* v = list of document vectors
   t = threshold
   minsize optimization, original one *)

let par_all_pairs_0_minsize v t =
  let t = Weight.of_float t in
  let o = ref (Docmatch_Set.make ()) in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world
    (* number of dimensions in data set *)
  and n = List.length v in  
  let maxweight_dv = max_weight_array 
    (Array.of_list (List.map max_weight_dv v)) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
    (* i is an inverted index, where i.(x) is an inverted list *)
  let i = Array.init m (fun ix->MySV.make ()) in  
  let v = List.sort (fun a b -> compare maxweight_dv.(b.docid) 
                       maxweight_dv.(a.docid)) v 
  and vsizelocal = Array.make n 0
  and vsize = Array.make n 0 in 
    if debug then lprintf "par-all-pairs-minsize\n";
    List.iter (fun x->vsizelocal.(x.docid) <- Array.length x.vector) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world; 
    if debug then 
      (lprintf "v = \n"; List.iter MyDV.lprint v;
       lprintf "maxweight_dim = \n"; 
       Array.iter (fun x-> lprintf "%f " (Weight.to_float x)) 
         maxweight_dim;
       lprintf "\nvsize = "; Array.iter (fun x-> lprintf "%d " x) vsize;
       lprintf "\n");
    List.iter
      (fun x ->
         if debug then lprintf "processing doc %d\n" x.docid; 
         o := Docmatch_Set.union !o 
           (par_find_matches_0_minsize x maxweight_dim vsize i t);
         Array.iter
           (fun dvelt->
              if debug2 then lprintf "checking term %d\n" dvelt.term;
              if debug2 then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
         if debug2 then
           (lprintf "\ni = \n"; 
            Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
              i)
      ) v;
    !o (* return output set*)

(*
  merge local a's across processors for 
  dimension-wise partitioning of vectors
  for an arbitrary topology
*)

let merge_scores_array ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let cl = ref [] in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > t_local then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
    let cl = List.sort compare (List.rev !cl) in
    if debug then lprintf "|cl|=%d\n" (List.length cl);
    let t0 = new timer in
    let cg = Parallel.reduce_all ~comm:comm cl Util.merge_sets [] in
    let al = ref [] in
      if profile then 
        (num_candidates := !num_candidates + (List.length cl);
         comm_time := !comm_time +.t0#elapsed);
      List.iter
        (fun y -> 
           (* if w>0g we have already calculated its local count*)
           let w = a.(y) in
             if w > Weight.zero then  
               al := (y, w) :: !al (* add (y,w) as a local count *)
        ) cg;
      if debug then lprintf "|cg|=%d\n" (List.length cg);
      let al_sorted = 
        List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
      let t1 = new timer in
      let scores_mine = accumulate_scores_fast2 ~comm:comm al_sorted
      in
        if profile then 
          (num_scores:=!num_scores + (List.length !al);
           my_scores:=!my_scores + (List.length scores_mine);
           comm_time2:=!comm_time2+.t1#elapsed;
          ); 
        List.iter
          (fun (y,s) -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) o
          ) scores_mine

(* crippleware *)

let merge_scores_array_nopruning ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let al = ref [] in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > Weight.zero then
           al := (y, w) :: !al (* add (y,w) as a local count *)
      ) a;
    let al_sorted = 
      List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
    let t1 = new timer in
    measure_barrier comm;
    let scores_mine = accumulate_scores_fast2 ~comm:comm al_sorted
    in
      if profile then 
        (num_scores:=!num_scores + (List.length !al);
         my_scores:=!my_scores + (List.length scores_mine);
         comm_time2:=!comm_time2+.t1#elapsed;
        ); 
      List.iter
        (fun (y,s) -> 
           if debug then lprintf "Sim of %d and %d = %f\n" x y 
             (Weight.to_float s); 
           if  s >= t then
             Docmatch_ISet.add (x, y, s) o
        ) scores_mine

let merge_scores_array_aapc ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let cl = ref [] in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > t_local then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
    let cl = List.sort compare (List.rev !cl) in
    if debug then lprintf "|cl|=%d\n" (List.length cl);
    let t0 = new timer in
    let cg = Parallel.reduce_all ~comm:comm cl Util.merge_sets [] in
    let al = ref [] in
      if profile then 
        (num_candidates := !num_candidates + (List.length cl);
         comm_time := !comm_time +.t0#elapsed);
      List.iter
        (fun y -> 
           (* if w>0g we have already calculated its local count*)
           let w = a.(y) in
             if w > Weight.zero then  
               al := (y, w) :: !al (* add (y,w) as a local count *)
        ) cg;
      if debug then lprintf "|cg|=%d\n" (List.length cg);
      let al_sorted = 
        List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
      let t1 = new timer in
      let scores_mine = accumulate_scores_aapc ~comm:comm al_sorted
      in
        if profile then 
          (num_scores:=!num_scores + (List.length !al);
           my_scores:=!my_scores + (List.length scores_mine);
           comm_time2:=!comm_time2+.t1#elapsed;
          ); 
        List.iter
          (fun (y,s) -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) o
          ) scores_mine

let lprint_dynarray printelt l =
  lprintf "[[ ";
  Dynarray.iter (fun x-> printelt x; lprintf "; ") l;
  lprintf " ]]"

let lprint_intdynarray l = lprint_dynarray lprint_int l

let lprint_floatdynarray l = lprint_dynarray lprint_float l

let lprint_wgtdynarray l = lprint_dynarray (fun x->lprint_float 
                                              (Weight.to_float x)) l

(* merge scores with recursive local pruning, requires 2^k processors 
   this is a naive implementation of the idea written in a functional style *)

let rec merge_scores_array_rec ?(comm=Mpi.comm_world) x a t =
  if debug then (lprintf "merge scores rec "; lprint_wgtarray a; 
		 lprintf " t=%g\n" (Weight.to_float t)) ;
  let m = DynarrayPair.make 0 Weight.zero in (* output matches *)
  let p = Mpi.comm_size comm and pid = Mpi.comm_rank comm in
    lprintf "in comm: p=%d, pid=%d\n" p pid;
    if p = 1 then (* basis of recursion *)
      (Array.iteri 
         (fun y w ->
            if w > t then
              DynarrayPair.append m y w; (* add y, w as a match *)
         ) a;

       m) (* return matches, sorted in increasing order of vec id's *)
    else 
      (* recursion: split mpi communicator and divide t by 2 *)
      let color = if in_range pid (0,p/2-1) then 0 else 1 in
      let comm' = Mpi.comm_split comm color pid in
      let m' = merge_scores_array_rec ~comm:comm' x a (t/2) in
      (* induction: m' holds matches with t/2, merge these scores *)
      (* compute candidate set by taking union of vec id's in m' *)
      let (aly,alw) = DynarrayPair.make 0 Weight.zero in
      let (my',mw') = m' in
      if debug then (lprintf "my'="; lprint_intdynarray my'; 
		     lprintf " mw'="; lprint_wgtdynarray mw'; lprintf "\n");
      let c = Parallel.reduce_all ~comm:comm my' merge_dynarray_sets  
        (Dynarray.make 0) in
	if debug then (lprintf "c="; lprint_intdynarray c; lprintf "\n");
        (* all processors of this group have all the candidates, 
           we now find local scores that contribute to candidates *)
        Dynarray.iter
          (fun y -> 
             let w = a.(y) in          (* if w>0 we have its local count *)
               if w > Weight.zero then (* add (y,w) as a local count *)
                 DynarrayPair.append (aly,alw) y w
          ) c; (* and accumulate contributing local scores *)
	if debug then (
	  lprintf "aly="; lprint_intdynarray aly; 
	  lprintf " alw="; lprint_wgtdynarray alw; lprintf "\n");
        let ag = accumulate_scores_pair_aapc ~comm:comm (aly, alw) in
	  lprintf "yay!\n";
	  DynarrayPair.iter (* iterate over scores in our group *)
            (fun y w ->
               if w > t then
		 DynarrayPair.append m y w; (* add y, w as a match *)
            ) ag; 
	  m (* return matches, sorted *)

(*
  merge local a's across processors for 
  dimension-wise partitioning of vectors
  for an arbitrary topology
  optimized to use array pairs instead of pair lists
*)
let merge_scores_array_opt ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let ca = Dynarray.make 0 in (* local candidate array *)
    Array.iteri
      (fun y w -> 
         if w > t_local then
           Dynarray.append ca y (* add y as a candidate *)
      ) a;
    Dynarray.sort compare ca;
    if debug then lprintf "|ca|=%d\n" (Dynarray.length ca);
    if profile then 
      num_candidates := !num_candidates + (Dynarray.length ca);
    let t0 = new timer in
    let cg = 
      Parallel.reduce_all ~comm:comm ca merge_dynarray_sets 
        (Dynarray.make 0) in
    let aly, alw = DynarrayPair.make 0 Weight.zero in
      if profile then comm_time := !comm_time +.t0#elapsed;
      Dynarray.iter
        (fun y -> 
           (* if w>0 we have already calculated its local count*)
           let w = a.(y) in
             if w > Weight.zero then (* add (y,w) as a local count *)
               (Dynarray.append aly y;
                Dynarray.append alw w)
        ) cg;
      if debug then lprintf "|cg|=%d\n" (Dynarray.length cg); 
      let al_sorted_y,al_sorted_w = 
        DynarrayPair.sort_first compare (aly,alw) in 
      let t1 = new timer in
      measure_barrier comm;
      let (scores_mine_y,scores_mine_w) = 
        accumulate_scores_pair_aapc ~comm:comm (al_sorted_y, al_sorted_w)
      in
        if profile then 
          (comm_time2:=!comm_time2+.t1#elapsed;
           num_scores:=!num_scores + (Dynarray.length aly);
           my_scores:=!my_scores + (Dynarray.length scores_mine_y);
          ); 
        DynarrayPair.iter 
          (fun y s -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) o
          ) (scores_mine_y, scores_mine_w) 

let merge_scores_array_opt_nopruning ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let aly, alw = DynarrayPair.make 0 Weight.zero in
    Array.iteri
      (fun y w -> 
         (* if w>0 we have already calculated its local count*)
         if w > Weight.zero then (* add (y,w) as a local count *)
           (Dynarray.append aly y;
            Dynarray.append alw w)
      ) a;
    let al_sorted_y,al_sorted_w = 
      DynarrayPair.sort_first compare (aly,alw) in
    let t1 = new timer in
    measure_barrier comm;
    let (scores_mine_y,scores_mine_w) = 
      accumulate_scores_pair_aapc ~comm:comm (al_sorted_y, al_sorted_w)
    in
      if profile then 
        (num_scores:=!num_scores + (Dynarray.length aly);
         my_scores:=!my_scores + (Dynarray.length scores_mine_y);
         comm_time2:=!comm_time2+.t1#elapsed;
        ); 
      DynarrayPair.iter 
        (fun y s -> 
           if debug then lprintf "Sim of %d and %d = %f\n" x y 
             (Weight.to_float s); 
           if  s >= t then
             Docmatch_ISet.add (x, y, s) o
        ) (scores_mine_y, scores_mine_w)

(*
  merge local a's across processors for 
  dimension-wise partitioning of vectors
*)
let hypercube_merge_scores_array ?(comm=Mpi.comm_world) x a t o = 
  let p = Mpi.comm_size comm in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let cl = ref [] in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > t_local then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
    let cl = List.sort compare (List.rev !cl) in
    if debug then lprintf "|cl|=%d\n" (List.length cl);
    let t0 = new timer in
    let cg = Parallel.hypercube_mnac_all cl Util.merge_sets  in
    let al = ref [] in
      if profile then 
        (comm_time := !comm_time +.t0#elapsed; 
         num_candidates := !num_candidates + (List.length cl));
      List.iter
        (fun y -> 
           (* if w>0 we have already calculated its local count*)
           let w = a.(y) in
             if w > Weight.zero then  
               al := (y, w) :: !al (* add (y,w) as a local count *)
        ) cg;
      if debug then lprintf "|cg|=%d\n" (List.length cg);
      let al_sorted = 
        List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
      let t1 = new timer in
      let scores_mine = hypercube_accumulate_scores al_sorted
      in
        if profile then 
          (num_scores:=!num_scores + (List.length !al);
           comm_time2:=!comm_time2+.t1#elapsed;
          ); 
        List.iter
          (fun (y,s) -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) o
          ) scores_mine


let hypercube_merge_scores_multi ?(comm=Mpi.comm_world) cands t o =
  let xid_list = Util.map (fun (x,c,cl) -> x) cands in
  let c_list = Util.map (fun (x,c,cl) -> c) cands in
  let cl_list = Util.map (fun (x,c,cl) -> cl) cands in 
  if debug then (
    lprintf "xid_list"; lprint_intlist xid_list; lprintf "\n";
    lprintf "candlists";
    List.iter lprint_intlist cl_list; lprintf "\n");
  measure_barrier comm;
  let t_s = new timer in 
  let cg_list = Parallel.hypercube_mnac_all cl_list 
    (fun cll1 cll2-> Util.map2 Util.merge_sets cll1 cll2)  
   (* (Util.repeat [] bsize) *)in
    if debug then (
      lprintf "global cands"; List.iter lprint_intlist cg_list;
      lprintf "\n");
    comm_time := !comm_time +. t_s#elapsed;
    if profile then lprintf "mnac_all: %g\n" t_s#elapsed;
    if profile then (
      let nc =  (sum (List.map List.length cl_list)) 
      and ns =  (sum (List.map List.length cg_list)) in
      num_candidates := !num_candidates + nc;
      num_scores := !num_scores +ns;
      lprintf "nc=%d, ns=%d\n" nc ns;
    );
    let al_list =      
      Util.map2
        (fun c c_g ->
           let al = ref [] in
             List.iter
               (fun y -> 
                  (* if weight>0 we have already calculated its count*)
                  let w=c.(y) in
                    if w>0 then al := (y, w) :: !al  (* add local score *)
               ) c_g;
             List.rev !al
        ) c_list cg_list in
      let t_s = new timer in
      (*let scores_mine_list = Parallel.accumulate_scores_fast2_multi
        al_list *)
      (*let scores_mine_list = Parallel.mnac_all al_list 
        (fun all1 all2 -> Util.map2 Parallel.merge_als all1 all2)*)
      measure_barrier comm;
      let scores_mine_list = hypercube_accumulate_scores_new_multi al_list  
      in
        comm_time2:=!comm_time2 +. t_s#elapsed;
        if profile then lprintf "accumulation: %g\n" t_s#elapsed;
        if debug then (
          lprintf "|x|=%d ,|scores|=%d\n" 
            (List.length xid_list) (List.length scores_mine_list); 
          List.iter (fun x->lprint_assoc_list x "al_list") al_list;
          List.iter (fun x->lprint_assoc_list x "scores") scores_mine_list; 
          lprintf "\n");
        List.iter2 
          (fun x scores_mine ->
             List.iter
               (fun (y,s) -> 
                  if  s >= t then
                    Docmatch_Vec.add (x, y, s) o
               ) scores_mine;
          ) xid_list scores_mine_list

let merge_scores_multi ?(comm=Mpi.comm_world) cands t o =
  let xid_list = Util.map (fun (x,c,cl) -> x) cands in
  let c_list = Util.map (fun (x,c,cl) -> c) cands in
  let cl_list = Util.map (fun (x,c,cl) -> cl) cands in 
  let bsize = List.length xid_list in
  if debug then (
    lprintf "xid_list"; lprint_intlist xid_list; lprintf "\n";
    lprintf "candlists";
    List.iter lprint_intlist cl_list; lprintf "\n");
  measure_barrier comm;
  let t_s = new timer in 
  let cg_list = Parallel.reduce_all cl_list 
    (fun cll1 cll2-> Util.map2 Util.merge_sets cll1 cll2)  
   (Util.repeat [] bsize) in
    if debug then (
      lprintf "global cands"; List.iter lprint_intlist cg_list;
      lprintf "\n");
    comm_time := !comm_time +. t_s#elapsed;
    if profile then lprintf "multi reduce_all: %g\n" t_s#elapsed;
    if profile then (
      let nc =  (sum (List.map List.length cl_list)) 
      and ns =  (sum (List.map List.length cg_list)) in
      num_candidates := !num_candidates + nc;
      (*num_scores := !num_scores +ns;*)
      lprintf "nc=%d\n" nc;
    );
    let al_list =      
      Util.map2
        (fun c c_g ->
           let al = ref [] in
             List.iter
               (fun y -> 
                  (* if weight>0 we have already calculated its count*)
                  let w=c.(y) in
                    if w>0 then al := (y, w) :: !al  (* add local score *)
               ) c_g;
             List.rev !al
        ) c_list cg_list in
      if profile then (
        let ns =  (sum (Util.map List.length al_list)) in
          num_scores := !num_scores +ns; lprintf "ns=%d\n" ns);
      let t_s = new timer in
      let scores_mine_list = accumulate_scores_fast2_multi al_list
      in
        comm_time2:=!comm_time2 +. t_s#elapsed;
        if profile then lprintf "accumulate scores: %g\n" t_s#elapsed;
        if debug then (
          lprintf "|x|=%d ,|scores|=%d\n" 
            (List.length xid_list) (List.length scores_mine_list); 
          List.iter (fun x->lprint_assoc_list x "al_list") al_list;
          List.iter (fun x->lprint_assoc_list x "scores") scores_mine_list; 
          lprintf "\n");
        List.iter2 
          (fun x scores_mine ->
             List.iter
               (fun (y,s) -> 
                  if  s >= t then
                    Docmatch_Vec.add (x, y, s) o
               ) scores_mine;
          ) xid_list scores_mine_list

let merge_scores_multi_pair ?(comm=Mpi.comm_world) cands t o =
  let xid_list = Util.map (fun (x,c,ca) -> x) cands in
  let c_list = Util.map (fun (x,c,ca) -> c) cands in
  let ca_list = Util.map (fun (x,c,ca) -> ca) cands in 
  let bsize = List.length xid_list in
  if debug then (
    lprintf "xid_list"; lprint_intlist xid_list; lprintf "\n";
    (*lprintf "candlists";
    List.iter lprint_intlist cl_list;*) lprintf "\n");
  if profile then (
    let t0 = new timer in
      Mpi.barrier comm; 
      barrier_time := !barrier_time +. t0#elapsed;
      lprintf "barrier time %g\n" t0#elapsed;
  );
  let t_s = new timer in 
  let cg_list = Parallel.reduce_all ~comm:comm ca_list 
    (fun cla1 cla2-> Util.map2 merge_dynarray_sets cla1 cla2)  
    (Util.construct (fun ()->Dynarray.make 0) bsize) in
    (*if debug then (
      lprintf "global cands"; List.iter Dynarray.lprint_int cg_list;
      lprintf "\n");*)
    comm_time := !comm_time +. t_s#elapsed;
    if profile then lprintf "multi reduce_all: %g\n" t_s#elapsed;
    if profile then (
      let nc =  (sum (Util.map Dynarray.length ca_list)) 
      and ns =  (sum (Util.map Dynarray.length cg_list)) in
      num_candidates := !num_candidates + nc; lprintf "nc=%d\n" nc
      (*num_scores := !num_scores +ns;*)
    );
    let al_list =      
      Util.map2
        (fun c c_g ->
           let al = DynarrayPair.make 0 Weight.zero in
             Dynarray.iter
               (fun y -> 
                  (* if weight>0 we have already calculated its count*)
                  let w=c.(y) in
                    if w>0 then 
                      DynarrayPair.append al y w (* add local score *)
               ) c_g;
             al
        ) c_list cg_list in
      if profile then (
        let ns =  (sum (Util.map DynarrayPair.length al_list)) in
          num_scores := !num_scores +ns; lprintf "ns=%d\n" ns);
      let t_s = new timer in
      let scores_mine_list = accumulate_scores_pair_multi al_list in
        comm_time2:=!comm_time2 +. t_s#elapsed;
        if profile then lprintf "accumulate scores: %g\n" t_s#elapsed;
        if debug then (
          lprintf "|x|=%d ,|scores|=%d\n" 
            (List.length xid_list) (Array.length scores_mine_list); 
          lprintf "\n");
        array_iter2
          (fun x scores_mine ->
             DynarrayPair.iter
               (fun y s -> 
                  if s >= t then
                    Docmatch_Vec.add (x, y, s) o
               ) scores_mine;
          ) (Array.of_list xid_list) scores_mine_list

let compute_scores_0_array dv n i =
  let a = Array.make n Weight.zero in
    Array.iter
      (fun dvelt->
         let xt = dvelt.term and xw = dvelt.freq in
           MySV.iter (fun (y, w)-> a.(y) <- a.(y) +: (xw *: w)) i.(xt); 
      ) dv.vector;
    if debug2 then
      (lprintf "\nlocal a =";
       Array.iter (fun x->lprintf "%f " (Weight.to_float x)) a);
    a

(* compute scores and matches *)
let compute_scores_matches_0_array dv n i t =
  let a = compute_scores_0_array dv n i in
  let cl = ref [] in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > t then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
    let cl = List.sort compare (List.rev !cl) in
      (a, cl)

(* compute scores and matches *)
let compute_scores_matches_0_array_pair dv n i t =
  let a = compute_scores_0_array dv n i in
  let ca = Dynarray.make 0 in (* local candidate list *)
    Array.iteri
      (fun y w -> 
         if w > t then
           Dynarray.append ca y (* add y as a candidate *)
      ) a;
    Dynarray.sort compare ca;
    (a, ca)

let par_find_matches_0_array_vert ?(comm=Mpi.comm_world) dv n i t =
  let x = dv.docid in
  let o = Docmatch_Vec.make () in
  let t0 = new timer in
    if debug then
      (lprintf "par_find_matches_0_array_vert %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    let a = compute_scores_0_array dv n i in
      comp_time := !comp_time +. t0#elapsed;
      merge_scores_array ~comm:comm x a t o;
      o


(* crippleware for performance comparison *)
let par_find_matches_0_array_vert_nopruning ?(comm=Mpi.comm_world) dv n i t =
  let x = dv.docid in
  let o = Docmatch_Vec.make () in
  let t0 = new timer in
    if debug then
      (lprintf "par_find_matches_0_vert_dense %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    let a = compute_scores_0_array dv n i in
      comp_time := !comp_time +. t0#elapsed;
      merge_scores_array_nopruning ~comm:comm x a t o;
      o


let par_find_matches_0_array_vert_rec ?(comm=Mpi.comm_world) dv n i t =
  let x = dv.docid in
  let t0 = new timer in
    if debug then
      (lprintf "par_find_matches_0_array_vert_rec %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    let a = compute_scores_0_array dv n i in
      comp_time := !comp_time +. t0#elapsed;
      merge_scores_array_rec ~comm:comm x a t

let par_find_matches_0_array_vert_aapc ?(comm=Mpi.comm_world) dv n i t =
  let x = dv.docid in
  let o = Docmatch_Vec.make () in
  let t0 = new timer in
    if debug then
      (lprintf "par_find_matches_0_vert_dense %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    let a = compute_scores_0_array dv n i in
      comp_time := !comp_time +. t0#elapsed;
      merge_scores_array ~comm:comm x a t o;
      o

(* feature-wise parallelization of all-pairs-0
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold  
   with local pruning of candidates
*)


let par_all_pairs_0_array_vert v t = 
  let t = Weight.of_float t in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
    if debug then lprintf "par-all-pairs-0-array-vert\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun ix x ->
         if ix mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" ix; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o (par_find_matches_0_array_vert x n i t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    o (* return output set*)

(* crippleware for some experiments
   feature-wise parallelization of all-pairs-0
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold  
   without local pruning 
*)

let par_all_pairs_0_array_vert_nopruning v t = 
  let t = Weight.of_float t in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
    if debug then lprintf "par-all-pairs-0-array-vert-nopruning\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun ix x ->
         if ix mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" ix; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o 
           (par_find_matches_0_array_vert_nopruning x n i t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    o (* return output set*)


(* recursive local pruning *)

let par_all_pairs_0_array_vert_rec v t = 
  let t = Weight.of_float t in
  (* m is the number of dimensions in the dataset *)
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
    if debug then lprintf "par-all-pairs-0-array-vert-rec\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun ix x ->
         if ix mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" ix; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         DynarrayPair.iter
           (fun y w -> Docmatch_Vec.add (x.docid,y,w) o)
           (par_find_matches_0_array_vert_rec x n i t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    o (* return output set*)

let par_all_pairs_0_array_vert_aapc v t = 
  let t = Weight.of_float t in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
    if debug then lprintf "par-all-pairs-0-vert-dense\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun ix x ->
         if ix mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" ix; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o 
           (par_find_matches_0_array_vert_aapc x n i t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    o (* return output set*)

let par_all_pairs_0_array_vert_opt v t = 
  let t = Weight.of_float t in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
  let vblocks = partition_vectors_blocksize v !blocksize in
    if debug then lprintf "par-all-pairs-0-array-vert-opt\n";
    if pid = 0 then printf "block size = %d\n" !blocksize;
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun bix vblock ->
         if pid=0 then (printf "#docs: %d    \r" (bix * !blocksize); 
                        flush stdout); 
         let t0 = new timer in
         let candidates = ref [] in
           List.iter
             (fun x -> 
                if debug0 then lprintf "doc id %d\n" x.docid; 
                if debug then lprintf "processing doc %d\n" x.docid; 
                let a,cl = compute_scores_matches_0_array x n i t_local in
                  candidates := (x.docid,a,cl) :: !candidates;
                  Array.iter
                    (fun dvelt-> 
                       MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                    ) x.vector;
             ) vblock;
           comp_time := !comp_time +. t0#elapsed;
           lprintf "compute scores %g sec\n" t0#elapsed;
           let t1 = new timer in 
             merge_scores_multi !candidates t o;
             lprintf "merge scores %g sec\n" t1#elapsed;
             (*Parallel.check_memory ()*)
      ) vblocks; 
    o (* return output set*)

let par_all_pairs_0_array_vert_opt_pair v t = 
  let t = Weight.of_float t in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
  let vblocks = partition_vectors_blocksize v !blocksize in
    if debug then lprintf "par-all-pairs-0-array-vert-opt-pair\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun vblock ->
         if pid=0 then (printf "."; flush stdout); 
         let t0 = new timer in
         let candidates = ref [] in
           List.iter
             (fun x -> 
                if debug then lprintf "processing doc %d\n" x.docid; 
                let a,ca = 
                  compute_scores_matches_0_array_pair x n i t_local in
                  candidates := (x.docid,a,ca) :: !candidates;
                  Array.iter
                    (fun dvelt-> 
                       MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                    ) x.vector;
             ) vblock;
           comp_time := !comp_time +. t0#elapsed;
           lprintf "compute scores %g sec\n" t0#elapsed;
           let t1 = new timer in 
             merge_scores_multi_pair !candidates t o;
             lprintf "merge scores %g sec\n" t1#elapsed;
      ) vblocks; 
    o (* return output set*)


(* this is not a good idea for ocaml *)
let par_all_pairs_0_array_vert_opt_mt v t = 
  let t = Weight.of_float t in
  let t_local = t/:(Weight.of_float (float_of_int p)) in  
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn)  in
  let vblocks = partition_vectors_blocksize v !blocksize in 
  let th_prev = ref None in 
  let join_th_prev () = 
    lprintf "joining previous thread\n";
    let t = new timer in
      match !th_prev with
          None -> lprintf "no prev thread";
        | Some th -> 
            lprintf "joining thread %d\n" (Thread.id th);
            Thread.join th;
            lprintf "waited %g sec for previous thread\n" t#elapsed
  in
    if debug then lprintf "par-all-pairs-0-array-vert-opt2\n";
    if profile then comm_time := 0.0;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun vblock ->
         if pid=0 then (printf "."; flush stdout);
         if profile then lprintf "|vblock|=%d\n" (List.length vblock); 
         let t0 = new timer in
         let candidates = ref [] in
           List.iter
             (fun x ->
                if debug then lprintf "processing doc %d\n" x.docid; 
                let a,cl = compute_scores_matches_0_array x n i t_local in
                  candidates := (x.docid,a,cl) :: !candidates;
                  Array.iter
                    (fun dvelt-> 
                       MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
                    ) x.vector;
             ) vblock;
           comp_time := !comp_time +. t0#elapsed;
           lprintf "compute scores %g sec\n" t0#elapsed;
           join_th_prev ();
           let t1 = new timer in 
           let th = Thread.create (* execute comms in a new thread *)
             (fun (candidates, t, o) ->
                lprintf "start thread %d\n" (Thread.id (Thread.self ()));
                hypercube_merge_scores_multi candidates t o;
                lprintf "stop thread\n";
             ) (!candidates, t, o) in
             Thread.yield (); (* start comms *)
             th_prev := Some th;
             lprintf "merge scores spawned in %g sec\n" t1#elapsed;
      ) vblocks;
    lprintf "waiting for the last round of comms to complete";
    join_th_prev ();
    o (* return output set*)

(* precond: assume a is a zero array of length n *)
let par_find_matches_0_vert_dense2 dv i a t =
  let o = Docmatch_Vec.make () in
  let x = dv.docid in 
  let aset = Dynarray.make_reserved 0 (Array.length a) in
    if debug then
      (lprintf "find_matches_0_vert_dense2 %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    Array.iter
      (fun dvelt->
         let xt = dvelt.term and xw = dvelt.freq in
           MySV.iter (fun (y, w)-> 
			if a.(y)=Weight.zero then
			  Dynarray.append aset y; (* maintain nz's *)
                        a.(y) <- a.(y) +: (xw *: w)) i.(xt); 
      ) dv.vector;
    
    if debug2 then
      (lprintf "\nlocal a =";
       Dynarray.iter (fun y->lprintf "%f " (Weight.to_float a.(y))) aset);
    
    (*
      merge a's across processors
    *)
    
    let t_local = t/:(Weight.of_float (float_of_int p)) in 
    let cl = ref [] in (* local candidate list *)
    Dynarray.iter
      (fun y ->
         if a.(y) > t_local then
           cl := (y:int) :: !cl; (* add y as a candidate *)
      ) aset;
      let cand_list = List.sort compare (List.rev !cl) in
      let t0 = new timer in 
      let c_g = Parallel.reduce_all cand_list Util.merge_sets [] in
      let al = ref [] in
        if profile then 
          (num_candidates := !num_candidates + (List.length cand_list);
           comm_time := !comm_time +. t0#elapsed);
        List.iter
          (fun y -> 
             (* if w>0 we have already calculated its local count*)
             let w = a.(y) in
               if w > Weight.zero then  
                 al := (y, w) :: !al (* add (y,w) as a local count *)
          ) c_g;
          let al_sorted = 
            List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
          let t1 = new timer in
          let scores_mine = accumulate_scores_fast2 al_sorted 
          in
            if profile then 
              (num_scores:=!num_scores + (List.length !al);
               comm_time2:=!comm_time2+.t1#elapsed);
            List.iter
              (fun (y,s) -> 
                 if debug then lprintf "Sim of %d and %d = %f\n" x y 
                   (Weight.to_float s); 
                 if  s >= t then
                   Docmatch_Vec.add (x, y, s) o
              ) scores_mine;

            (* clear array a *)
            Dynarray.iter
              (fun y-> a.(y) <- Weight.zero) aset;
            o (* return matches *)
  

(* feature-wise parallelization of all-pairs-0
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold  *)

let par_all_pairs_0_vert_dense2 v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let n = List.length v in
  let a = Array.make n Weight.zero in
    if debug then lprintf "par-all-pairs-0-vert-dense2\n";
    (* i is an inverted index, where i.(x) is an inverted list *)
    list_iteri
      (fun ix x ->
         if ix mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" ix; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o 
          (par_find_matches_0_vert_dense2 x i a t);
         Array.iter
           (fun dvelt->
              if debug then lprintf "checking term %d\n" dvelt.term;
              if debug then lprintf "adding (%d, %f)\n" x.docid 
                (Weight.to_float dvelt.freq);
              MySV.append i.(dvelt.term) (x.docid, dvelt.freq)
           ) x.vector;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    o (* return output set*)

            

(*
  merge a's across processors, assuming partial indexing
*)
let merge_scores_partial_indexing dv a v' order t =
  let o = Docmatch_Vec.make () in
  let x = dv.docid in
  let al = ref []
  and ag = Hashtbl.create (Hashtbl.length a) in
    Hashtbl.iter (fun y w -> al := (y, w) :: !al) a;
    let al_cand = hypercube_par_merge_assoc_lists (List.rev !al) in
      if profile then 
        (lprintf "|al|=%d, |al_cand|=%d\n" 
           (List.length !al) (List.length al_cand));
      List.iter (fun (y,s) -> Hashtbl.add ag y s) al_cand;
      let al_dotp = Util.map         
        (fun (y,s) -> (y, (MyDV.dot dv v'.(y) order))) al_cand in
      let al_dotp_g = accumulate_scores_fast2 al_dotp in
        List.iter
          (fun (y,dp) -> 
             let s = (Hashtbl.find ag y) +: dp in 
               if debug then
                 lprintf "Sim of %d and %d = %f\n" x y (Weight.to_float s); 
               if s >= t then
                 Docmatch_Vec.add (x, y, s) o;
          ) al_dotp_g;
        o
          

(*
  merge local a's across processors assuming partial indexing
  rationale: it's faster to brute-force all partial dot-products
  before trying to communicate anything.
*) 
let merge_scores_hashtbl_partial_indexing dv a v' order maxweight_dim t =  
  let o = Docmatch_ISet.make () in
  let x = dv.docid in
  let dot'= Array.make x Weight.zero in
  let t_local = t/:(Weight.of_float (float_of_int p)) in 
    (* TODO:  faster to distribute output *) 
  let cl = ref [] in (* local candidate list *)
  (*let remscore = Array.fold_left (+:) Weight.zero 
    (Array.map (fun e->e.freq *: maxweight_dim.(e.term)) dv.vector) in*)
  (*let len = MyDV.magnitude dv in*)
    Hashtbl.iter
      (fun y w -> 
         let dot = MyDV.dot dv v'.(y) order in
           dot'.(y) <- dot; 
           if w +: dot > t_local then 
             cl := (y:int) :: !cl; (* add y as a candidate *)
      ) a;
    for y=0 to x-1 do
      if dot'.(y) = Weight.zero then (
        (*TODO: more sophisticated attack on this problem,
          if |dv|=l, at least one inner product component  
          a[i]*b[i]>t_local/l *)
        let dot = MyDV.dot dv v'.(y) order in
          dot'.(y) <- dot;
      (*
        let leny' = MyDV.magnitude v'.(y) in
        lprintf "dot <' %f\n" (Pervasives.sqrt (Weight.to_float (len *: leny'))); 
        if (MyDV.length x.vector) > 1 then
        lprintf "<' %f\n" (Pervasives.sqrt (Weight.to_float (len *: leny'))); *)
          (*for i=0 to (MyDV.length dv)-1 do
            lprintf "%f \n" (Weight.to_float )*)
            
          (*lprintf "tlocal/l=%f " ((Weight.to_float t_local) /. l);
          lprintf "remscore=%f dot=%f\n" (Weight.to_float remscore)
            (Weight.to_float dot);*)
          if dot > t_local then
            cl := (y:int) :: !cl (* add y as a candidate *)
      )
    done;
    if debug then (lprintf "dot'="; lprint_wgtarray dot'; lprintf "\n");
    let c_l = List.sort compare (List.rev !cl) in
    let t0 = new timer in
    let c_g = Parallel.hypercube_mnac_all c_l Util.merge_sets in
    let al = ref [] in
      if debug then
        (lprintf "c_l =  "; lprint_intlist c_l;
         lprintf "c_g =  "; lprint_intlist c_g);
      if profile then 
        (num_candidates := !num_candidates + (List.length c_l);
         comm_time := !comm_time +. t0#elapsed);
      List.iter
        (fun y -> (* if weight>0 we have already calculated its local count*)
           let w = 
             if Hashtbl.mem a y then
               Hashtbl.find a y +: dot'.(y) 
             else
               dot'.(y)
           in 
             al := (y, w) :: !al (* add (y,w) as a local count *) 
        ) c_g; 
      (*lprint_assoc_list !al "al";*)
      let al_sorted = 
        List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in 
      let t1 = new timer in
      let scores_mine = accumulate_scores_fast2 al_sorted
      in
        if debug then
          (lprint_assoc_list al_sorted "al_sorted";
           lprint_assoc_list scores_mine "scores_mine"; 
           lprintf "\n");
        if profile then 
          (num_scores:=!num_scores + (List.length !al);
           comm_time2:=!comm_time2+.t1#elapsed);
        List.iter
          (fun (y,s) -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) o
          ) scores_mine;
        o

let par_find_matches_1_vert dv order v' i maxweight_dim t =
  let x = dv.docid in
  let n = Array.length v' in
  let a = Hashtbl.create (n/2) in
    if debug then (lprintf "par_find_matches_1_vert %d, length=%f, dv= " x 
                     (Weight.to_float (MyDV.magnitude dv)); MyDV.lprint dv);
    Array.iter
      (fun dvelt->
         let tw = dvelt.freq in
           MySV.iter 
             (fun (y, yw)->
                if debug then lprintf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  tw *: yw)
                else
                  Hashtbl.add a y (tw *: yw);
             )
             i.(dvelt.term);
      ) dv.vector;

    if debug then ( lprintf "\nlocal a = "; print_candidates a);

    merge_scores_hashtbl_partial_indexing dv a v' order maxweight_dim t
      
    (* let c = Hashtbl.create (n/2) in
    let o = Docmatch_Vec.make () in
      Hashtbl.iter (
        fun y w -> 
          Hashtbl.add c y (w +: (MyDV.dot dv v'.(y) order)) ) a;

      if debug then ( lprintf "\nlocal c = "; print_candidates a);
      merge_scores_hashtbl x c t o;
      o*)
          

(* feature-wise parallelization of all-pairs-1
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold 
*)


(* implementation with global pruning

   the partial indexing optimization is parallelized as follows.
   each processor locally orders dimensions in given local v in the order
    of decreasing number of nonzeros along a dimension.
   during indexing:
     * upper bound array myb is computed as prefix sum of 
       maxweight_i(term)*freq's in the local order of features.
     * each processor computes a global upper bound b by summing local upper 
       bounds then prunes according to global upper bound
   during parallel matching:
     * each processor adds local dot products to weights in candidate map
     * since each processor has some features of a global x[i] this is correct
       as in score accumulation, the weights are added.
 *)


let prefix_sum_fix32 a b =
  if debug then assert ((Array.length b) >= (Array.length a));
  let ps = ref Weight.zero in
    Array.iteri (fun i x -> 
                   ps := !ps +: x; 
                   b.(i) <- !ps) a


let par_all_pairs_1_vert v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Vec.make () in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let n = List.length v in
  (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
  let order = reorder_dims v m
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in 
  let sum_arrays a b = 
    if debug then assert((Array.length a)=(Array.length b));
    Array.init (Array.length a) (fun i->a.(i) +: b.(i)) in
    if debug then lprintf "par-all-pairs-1-vert\n";
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun x ->
         if x.docid mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" x.docid; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o (par_find_matches_1_vert x order v' i 
                                  maxweight_dim t);
         if debug then 
           lprintf "calculating b's, |x|=%d\n" (Array.length x.vector);
         (* calculate all b's in parallel beforehand *)
         let myxsize = (Array.length x.vector) in
         let xsize = Mpi.allreduce_int myxsize Mpi.Int_max Mpi.comm_world in
         let b = Array.make xsize Weight.zero and unindexed = ref [] in
           if debug then lprintf "myxsize=%d, xsize=%d\n" myxsize xsize;
           prefix_sum_fix32 
             (Array.init myxsize 
                (fun i->maxweight_dim.(x.vector.(i).term) *: x.vector.(i).freq))
             b;
           array_iter_range 
             (fun i x-> b.(i) <- b.(max (myxsize-1) 0)) b (myxsize, xsize-1);
           (* calculate all b's beforehand *)
           if debug then 
             lprint_array (fun x->lprintf "%f" (Weight.to_float x)) b;
           let b_global = Parallel.hypercube_mnac_all b sum_arrays in
             if debug then 
               (lprintf "mnac sum done, b_global=\n";
                lprint_array (fun x->lprintf "%f" (Weight.to_float x)) b);
             Array.iteri
               (fun ix dvelt-> (*TODO: use array funs for unindexed below*)
                  if debug then lprintf "checking term %d\n" dvelt.term;
                  if b_global.(ix) >=: t then (* NOTE: global pruning *)
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                  else
                    unindexed := (dvelt.term, dvelt.freq) :: !unindexed
               ) x.vector;
             v'.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    o (* return output set*)


let par_find_matches_1_vert_dense dv order v' i t =
  let x = dv.docid
  and n = Array.length v' in
  let a = Array.make n Weight.zero 
  and o = Docmatch_Vec.make () in
    if debug then 
      (lprintf "par_find_matches_1_vert_dense %d, length=%f, dv= " x 
         (Weight.to_float (MyDV.magnitude dv)); MyDV.lprint dv);
    Array.iter
      (fun dvelt->
         let xt = dvelt.term and xw = dvelt.freq in
           MySV.iter (fun (y, yw)-> a.(y) <- a.(y) +: (xw *: yw)) i.(xt); 
      ) dv.vector;
    if debug2 then
      (lprintf "\nlocal a =";
       Array.iter (fun x->lprintf "%f " (Weight.to_float x)) a);
    Array.iteri
      (fun y w ->
         if a.(y) > Weight.zero then
           a.(y) <- a.(y) +: (MyDV.dot dv v'.(y) order)) a;
    merge_scores_array x a t o;
    o
            

(* feature-wise parallelization of all-pairs-1
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold 
*)

let par_all_pairs_1_vert_dense v t = 
  let t = Weight.of_float t in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  (* m is the number of dimensions in the dataset *)
  let o = Docmatch_Vec.make_n (n*logn) in
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
  let order = reorder_dims v m
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in 
  let sum_arrays a b = 
    if debug then assert((Array.length a)=(Array.length b));
    Array.init (Array.length a) (fun i->a.(i) +: b.(i)) in
    if debug then lprintf "par-all-pairs-1-vert\n";
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun x ->
         if x.docid mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" x.docid; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         Docmatch_Vec.extend o 
           (par_find_matches_1_vert_dense x order v' i t);
         if debug then 
           lprintf "calculating b's, |x|=%d\n" (Array.length x.vector);
         (* calculate all b's in parallel beforehand *)
         let myxsize = (Array.length x.vector) in
         let xsize = Mpi.allreduce_int myxsize Mpi.Int_max Mpi.comm_world in
         let b = Array.make xsize Weight.zero and unindexed = ref [] in
           if debug then lprintf "myxsize=%d, xsize=%d\n" myxsize xsize;
           prefix_sum_fix32 
             (Array.init myxsize 
                (fun i->maxweight_dim.(x.vector.(i).term) *: x.vector.(i).freq))
             b;
           array_iter_range 
             (fun i x-> b.(i) <- b.(max (myxsize-1) 0)) b (myxsize, xsize-1);
           (* calculate all b's beforehand *)
           if debug then 
             lprint_array (fun x->lprintf "%f" (Weight.to_float x)) b;
           let b_global = Parallel.hypercube_mnac_all b sum_arrays in
             if debug then 
               (lprintf "mnac sum done, b_global=\n";
                lprint_array (fun x->lprintf "%f" (Weight.to_float x)) b);
             Array.iteri
               (fun ix dvelt-> (*TODO: use array funs for unindexed below*)
                  if debug then lprintf "checking term %d\n" dvelt.term;
                  if b_global.(ix) >=: t then (* NOTE: global pruning *)
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                  else
                    unindexed := (dvelt.term, dvelt.freq) :: !unindexed
               ) x.vector;
             v'.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x) i);
    o (* return output set*)


(* implementation with local pruning of b, not that fast because there 
is too much communication *)
let par_all_pairs_1_vert_local_pruning v t = 
  let t = Weight.of_float t 
  and t_local = Weight.of_float (t /. (float_of_int p)) in
  let o = ref (Docmatch_Set.make ()) in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let n = List.length v in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) in 
  let order = reorder_dims v m
  and v' = Array.init n (fun ix->MyDV.make_emptydv ()) in 
    if debug then lprintf "par-all-pairs-1-vert-local-pruning\n";
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun x ->
         if x.docid mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" x.docid; flush stdout);
         if debug then lprintf "processing doc %d\n" x.docid; 
         o := Docmatch_Set.union !o (par_find_matches_1_vert x order v' i
                                       maxweight_dim t);
         let b = ref Weight.zero and unindexed = ref [] in
           Array.iter
             (fun dvelt->
                if debug then lprintf "checking term %d\n" dvelt.term;
                b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                if !b >=: t_local then (*NOTE:local pruning for b *)
		  MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                else
                  unindexed := (dvelt.term, dvelt.freq) :: !unindexed
             ) x.vector;
           v'.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    !o (* return output set*)


let par_find_matches_1_minsize_vert dv i t order vp maxweight_dv vsize =
  let t_s = Mpi.wtime () in
  let x = dv.docid
  and tf = Weight.to_float t in
  let t_local = Weight.of_float (tf /. (float_of_int p)) in
  let n = Array.length vp in
  let logn = int_of_float ( Pervasives.log (float_of_int n) ) in
  let a = Hashtbl.create (n/2) 
  and c = Hashtbl.create (n/2) 
  and m = ref (Docmatch_Set.make_n (n * logn)) in
  let maxweightdv = maxweight_dv.(dv.docid) in 
  if maxweightdv=Weight.zero then !m else
  let minsize = tf /. (Weight.to_float (max_weight_dv dv)) in
    if debug then
      (lprintf "par_find_matches_1_minsize_vert %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    Array.iter
      (fun dvelt->

         let ti = dvelt.term and xw = dvelt.freq in
 
           (* remove docs in the front that are smaller than minsize *)
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize  do
               MySV.pop i.(ti)
           done;

           MySV.iter 
             (fun (y, yw)->
                if debug then lprintf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                else
                  Hashtbl.add a y (xw *: yw);
             )
             i.(ti);
      ) dv.vector;
    
    if debug then (
      lprintf "\nlocal a = "; print_candidates a;
    );

    (*
      merge a's across processors
    *)

    (* TODO? personalized communication, each processor sends
       (candidate, pid) and the pid's of each sending processor 
       are accumulated in a bitvector *)

    let cl = ref [] in (* local candidate list *)
      Hashtbl.iter
        (fun y w ->
           let weight = w +: (MyDV.dot dv vp.(y) order) in
             if weight > t_local then
               cl := (y:int) :: !cl; (* add y as a candidate *)
             Hashtbl.add c y weight; (* memoize all weights *)
        )
        a; 
      let cand_list = List.sort compare (List.rev !cl) in
      if profile then 
        ( comp_time := !comp_time +. (Mpi.wtime ()) -. t_s);
      let t0 = new timer in
      let c_g = Parallel.reduce_all cand_list Util.merge_sets []
      in (* global *)
      (*let c_g = Parallel.hypercube_merge_assoc_lists (List.rev !cl)in*) 
      let al = ref [] in
        if profile then 
          (num_candidates := !num_candidates + (List.length cand_list);
           comm_time := !comm_time +. t0#elapsed);
        List.iter
          (fun y -> (* if weight>0 we have already calculated its local count*)
             if Hashtbl.mem c y then
               let w = Hashtbl.find c y in (*recall*) 
                 al := (y, w) :: !al (* add (y,w) as a local count *)
          )
          c_g;
        let al_sorted = 
          List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in
        let t1 = new timer in
        let scores_mine = hypercube_accumulate_scores al_sorted in
          if profile then 
            (num_scores:=!num_scores + (List.length !al);
             comm_time2:=!comm_time2+.t1#elapsed); 
          (*if profile then lprintf "|al_mine|=%d\n" (List.length !al);*)
          List.iter
            (fun (y,s) -> 
               if debug then lprintf "Sim of %d and %d = %f\n" x y 
                 (Weight.to_float s); 
               if  s >= t then
                 m := Docmatch_Set.add (x, y, s) !m;
            ) scores_mine;
          !m


(* implementation with global pruning *)
let par_all_pairs_1_minsize_vert v t = 
  let t = Weight.of_float t in
  let o = ref (Docmatch_Set.make ()) in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let n = List.length v in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dv = max_weight_array (Array.of_list (List.map max_weight_dv v))
  in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) 
  and order = reorder_dims v m
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and vsizelocal = Array.make n 0
  and vsize = Array.make n 0 in 
    if profile then 
      (num_candidates := 0; comm_time := 0.0;
       num_scores := 0; comm_time2 := 0.0; comp_time := 0.0;
       let dim_nz = calc_dim_nz v m in
       let nz = Array.fold_left (+) 0 dim_nz 
       and work = 
         Array.fold_left (+.) 0.0 
           (Array.map (fun x->let y=float_of_int x in y*.y) dim_nz) in
         lprintf "num_nz=%d, work=%g\n" nz work;
      );
    if debug then lprintf "par-all-pairs-1-minsize-vert\n";
    List.iter (fun x->vsizelocal.(x.docid) <- Array.length x.vector) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun x ->
         let t_s = time () in
         if debug then lprintf "processing doc %d\n" x.docid;
         o := Docmatch_Set.union !o 
           (par_find_matches_1_minsize_vert x i t order vp maxweight_dv vsize);
         if profile then
           find_matches_time := !find_matches_time +. (time ()-.t_s);
         (* calculate all b's in parallel beforehand *)
         let myxsize = (Array.length x.vector) in
         let xsize = Mpi.allreduce_int myxsize Mpi.Int_max Mpi.comm_world in
         let b = Array.make xsize Weight.zero and unindexed = ref [] in
           prefix_sum_fix32 
             (Array.init myxsize 
                (fun i->maxweight_dim.(x.vector.(i).term) *: x.vector.(i).freq))
             b;
           array_iter_range 
             (fun i x-> b.(i) <- b.(max (myxsize-1) 0)) b (myxsize, xsize-1);
           let sum a b = 
             if debug then assert((Array.length a)=(Array.length b));
             Array.init (Array.length a) (fun i->a.(i) +: b.(i)) 
           in   
           let b_global = Parallel.hypercube_mnac_all b sum in
             (*if profile then lprintf "|b_global|=%d\n" (Array.length b_global);*)
             Array.iteri
               (fun ix dvelt->
                  if debug then lprintf "checking term %d\n" dvelt.term;
                  if b_global.(ix) >=: t then (* NOTE: global pruning *)
                    MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                  else
                    unindexed := (dvelt.term, dvelt.freq) :: !unindexed
               ) x.vector;
             vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
      ) v;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    if profile then 
      (lprintf "num cands = %d, comm_time = %g\n" !num_candidates !comm_time;
       lprintf "num scores = %d, comm_time2 = %g\n" !num_scores !comm_time2;
       lprintf "comp_time = %g\n" !comp_time;
       lprintf "gather time = %g, barrier_time = %g, find_matches =%f\n"
         !Parallel.gather_time !Parallel.barrier_time !find_matches_time);
    !o (* return output set*)

let par_find_matches_1_minsize_vert_phase1 dv i t order vp maxweight_dv vsize =
  let x = dv.docid
  and tf = Weight.to_float t in
  let t_local = Weight.of_float (tf /. (float_of_int p)) 
  and n = Array.length vp in
  let a = Hashtbl.create (n/2) 
  and c = Hashtbl.create (n/2) in
  let maxweightdv = maxweight_dv.(dv.docid) in 
  if maxweightdv=Weight.zero then (x,c,[]) else
  let minsize = tf /. (Weight.to_float (max_weight_dv dv)) in
    if debug then
      (lprintf "par_find_matches_1_minsize_vert_phase1 %d, length=%f " x 
         (Weight.to_float (MyDV.magnitude dv));
       lprintf "dv = "; MyDV.lprint dv;
      );
    Array.iter
      (fun dvelt->

         let ti = dvelt.term and xw = dvelt.freq in
 
           (* remove docs in the front that are smaller than minsize *)
           while MySV.length i.(ti) > 0 && 
             (float_of_int vsize.(fst (MySV.front i.(ti) ))) < minsize  do
               MySV.pop i.(ti)
           done;

           MySV.iter 
             (fun (y, yw)->
                if debug then lprintf "(%d, %f) " y (Weight.to_float yw);
                if Hashtbl.mem a y then
                  Hashtbl.replace a y ((Hashtbl.find a y) +:  xw *: yw)
                else
                  Hashtbl.add a y (xw *: yw);
             )
             i.(ti);
      ) dv.vector;
    
    if debug then (
      lprintf "\nlocal a = "; print_candidates a;
    );

    (*
      merge a's across processors
    *)

    (* TODO? personalized communication, each processor sends
       (candidate, pid) and the pid's of each sending processor 
       are accumulated in a bitvector *)

    let cl = (ref []: int list ref) in (* local candidate list *)
      Hashtbl.iter
        (fun y w ->
           let weight = w +: (MyDV.dot dv vp.(y) order) in
             if weight > t_local then
               cl := (y:int) :: !cl; (* add y as a candidate *)
             Hashtbl.add c y weight; (* memoize all weights *)
        )
        a; 
      let cand_list = List.sort compare (List.rev !cl) in
        (x, c, cand_list)

let par_find_matches_1_minsize_vert_phase2 x i t c cand_list m =
  let t_s = time () in 
  let c_g = Parallel.reduce_all cand_list Util.merge_sets [] in
  let al = ref [] in
    if profile then 
      (num_candidates := !num_candidates + (List.length cand_list);
       num_cmap := !num_cmap + (Hashtbl.length c);
       comm_time := !comm_time +.(time () -. t_s));
    List.iter
      (fun y -> (* if weight>0 we have already calculated its local count*)
         if Hashtbl.mem c y then
           let w = Hashtbl.find c y in (*recall*) 
             al := (y, w) :: !al (* add (y,w) as a local count *)
      )
      c_g;
    let t_s = time () in 
      let al_sorted = 
        List.sort (fun (y1,_) (y2,_)->compare y1 y2) (List.rev !al) in
      let scores_mine = hypercube_accumulate_scores al_sorted in
        if profile then 
          (num_scores:=!num_scores + (List.length !al);
           comm_time2:=!comm_time2+.(time () -. t_s);
           (*lprint_assoc_list al_sorted "sorted";*)
          );
        (*if profile then lprintf "|al_mine|=%d\n" (List.length !al);*)
        List.iter
          (fun (y,s) -> 
             if debug then lprintf "Sim of %d and %d = %f\n" x y 
               (Weight.to_float s); 
             if  s >= t then
               Docmatch_ISet.add (x, y, s) m
          ) scores_mine 

let par_find_matches_1_minsize_vert_phase2_multi i t cands m  =
  let xid_list = Util.map (fun (x,c,cl) -> x) cands in
  let c_list = Util.map (fun (x,c,cl) -> c) cands in
  let cand_list_list = Util.map (fun (x,c,cl) -> cl) cands in 
  let t_s = time () in 
  let c_g_list = Parallel.reduce_all cand_list_list 
    (fun cll1 cll2->Util.map2 Util.merge_sets cll1 cll2) [] in
  let al_list = ref [] in 
    if profile then 
      (
        (*num_candidates := !num_candidates +
         sum_list List.map (List.length cand_list_list);
       num_cmap := !num_cmap + (Hashtbl.length c);*)
        comm_time := !comm_time +.(time () -. t_s));
    List.iter2
      (fun c c_g ->
         let al = ref [] in
         List.iter
           (fun y -> (* if weight>0 we have already calculated its count*)
              if Hashtbl.mem c y then
                let w = Hashtbl.find c y in (*recall*) 
                  al := (y, w) :: !al (* add (y,w) as a local count *)) c_g;
           al_list := !al :: !al_list
      ) c_list c_g_list;
    let t_s = time () in 
    let al_sorted_l = Util.map List.rev !al_list in
    let scores_mine_list = Parallel.hypercube_mnac_all
      al_sorted_l (fun all1 all2 -> Util.map2 Score.merge_als all1 all2)
        (*let scores_mine = Parallel.hypercube_accumulate_scores_range 
          al_sorted (0,dims-1)*)
    in
      if profile then 
        ((*num_scores:=!num_scores + (List.length !al_sorted_l);*)
          comm_time2:=!comm_time2+.(time () -. t_s);
          (*lprint_assoc_list al_sorted "sorted";*)
        );
      (*if profile then lprintf "|al_mine|=%d\n" (List.length !al);*)
      List.iter2 
        (fun x scores_mine ->
           List.iter
             (fun (y,s) -> 
                if debug then lprintf "Sim of %d and %d = %f\n" x y 
                  (Weight.to_float s); 
                if  s >= t then
                  Docmatch_ISet.add (x, y, s) m
             ) scores_mine;
        ) xid_list scores_mine_list
        
let par_find_matches_1_minsize_vert_phase2_list i t candidates m =
  (*let m = ref (Docmatch_Set.make ()) in*)
    List.iter  
      (fun (x, c, cand_list) -> 
         par_find_matches_1_minsize_vert_phase2 x i t c cand_list m)
      candidates

let par_find_matches_1_minsize_vert2 dv i t order vp maxweight_dv vsize =
   let (x,c,cl) =  
    par_find_matches_1_minsize_vert_phase1 dv i t order vp maxweight_dv vsize 
  in
    par_find_matches_1_minsize_vert_phase2_list i t [(x,c,cl)]

(* local pruning parallelization applied to candidate generation 
   and partial indexing 
TODO: make it work with multi
*)
let par_all_pairs_1_minsize_vert2 v t = 
  let t = Weight.of_float t 
  and t_local = Weight.of_float (t /. (float_of_int p)) in
  let o = Docmatch_ISet.make () in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let n = List.length v in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dv = 
    max_weight_array (Array.of_list (List.map max_weight_dv v)) in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) 
  and order = reorder_dims v m
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and vsizelocal = Array.make n 0
  and vsize = Array.make n 0 in 
  let vblocks = partition_vectors_blocksize v !blocksize in 
    if profile then 
      (num_candidates := 0; comm_time := 0.0; tot_comm_time := 0.0; 
       num_scores := 0; comm_time2 := 0.0; comp_time := 0.0;
       let dim_nz = calc_dim_nz v m in
       let nz = Array.fold_left (+) 0 dim_nz 
       and work = 
         Array.fold_left (+.) 0.0 
           (Array.map (fun x->let y=float_of_int x in y*.y) dim_nz) in
         lprintf "num_nz=%d, work=%g\n" nz work;
      );
    if debug then lprintf "par-all-pairs-1-minsize-vert2\n";
    List.iter (fun x->vsizelocal.(x.docid) <- Array.length x.vector) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter
      (fun vblock ->
         if pid=0 then (printf "."; flush stdout);
         let candidates = ref [] in
         List.iter
           (fun x ->
              let t_s = time () in 
              if debug then lprintf "processing doc %d\n" x.docid;
              let xid,c,cl = par_find_matches_1_minsize_vert_phase1 
                x i t order vp maxweight_dv vsize in
                candidates := (xid,c,cl) :: !candidates; 
                if profile then 
                  find_matches_time := !find_matches_time +. (time ()-.t_s);
                let b = ref Weight.zero and unindexed = ref [] in
                  Array.iteri
                    (fun ix dvelt->
                       if debug then lprintf "checking term %d\n" dvelt.term;
                       b := !b +: maxweight_dim.(dvelt.term) *: dvelt.freq;
                       if !b >=: t_local then (* local pruning for b bound *)
                         MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                       else
                         unindexed := (dvelt.term, dvelt.freq) :: !unindexed
                    ) x.vector;
                  vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
                  comp_time := !comp_time +. (time () -. t_s)
           ) vblock; 
           let t_s = time () in 
             par_find_matches_1_minsize_vert_phase2_list i t !candidates o;
             tot_comm_time := !tot_comm_time +. (time () -. t_s);
      ) vblocks;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    if profile then 
      (lprintf "num cands = %d, num_cmap = %d, comm_time = %g\n" 
         !num_candidates !num_cmap !comm_time;
       lprintf "num scores = %d, comm_time2 = %g\n" !num_scores !comm_time2;
       lprintf "comp_time = %g, tot_comm_time=%g\n" !comp_time !tot_comm_time;
       lprintf "gather time = %g, barrier_time = %g, find_matches =%f\n"
         !Parallel.gather_time !Parallel.barrier_time !find_matches_time);
    o (* return output set*)

(* implementation with global pruning of b*)
let par_all_pairs_1_minsize_vert3 v t = 
  let t = Weight.of_float t in
  let o = Docmatch_Set.make () in
  let m = Mpi.allreduce_int (1 + max_dim v) Mpi.Int_max Mpi.comm_world in
  let n = List.length v in
    (* m is the number of dimensions in the dataset *)
  let i = Array.init m (fun ix->MySV.make ()) in
  let maxweight_dv = max_weight_array (Array.of_list (List.map max_weight_dv v))
  in
  let maxweight_dim = max_weight_array (maxweight_dim_preprocess v m) 
  and order = reorder_dims v m
  and vp = Array.init n (fun ix->MyDV.make_emptydv ()) 
  and vsizelocal = Array.make n 0
  and vsize = Array.make n 0 in
  let vblocks = partition_vectors_blocksize v !blocksize in  
    if profile then 
      (num_candidates := 0; comm_time := 0.0;
       num_scores := 0; comm_time2 := 0.0; comp_time := 0.0;
       let dim_nz = calc_dim_nz v m in
       let nz = Array.fold_left (+) 0 dim_nz 
       and work = 
         Array.fold_left (+.) 0.0 
           (Array.map (fun x->let y=float_of_int x in y*.y) dim_nz) in
         lprintf "num_nz=%d, work=%g\n" nz work;
      );
    if debug then lprintf "par-all-pairs-1-minsize-vert\n";
    List.iter (fun x->vsizelocal.(x.docid) <- Array.length x.vector) v;
    Mpi.allreduce_int_array vsizelocal vsize Mpi.Int_sum Mpi.comm_world;
    (* i is an inverted index, where i.(x) is an inverted list *)
    List.iter 
      (fun vblock ->
         if pid=0 then (printf "."; flush stdout);
         let candidates = ref [] in
           List.iter
           (fun x ->
              let t_s = time () in
                Mpi.barrier Mpi.comm_world;
                barrier_time2 := !barrier_time2 +. (time ()-.t_s);
                if debug then lprintf "processing doc %d\n" x.docid; 
                let t1 = new timer in
                let xid,c,cl = par_find_matches_1_minsize_vert_phase1 
                  x i t order vp maxweight_dv vsize in
                  candidates := (xid,c,cl) :: !candidates; 
                if profile then
                  find_matches_time := !find_matches_time +. t1#elapsed;
                (* calculate all b's in parallel beforehand *)
                let myxsize = (Array.length x.vector) in
                let xsize = Mpi.allreduce_int myxsize Mpi.Int_max Mpi.comm_world in
                let b = Array.make xsize Weight.zero and unindexed = ref [] in
                  prefix_sum_fix32 
                    (Array.init myxsize 
                       (fun i->maxweight_dim.(x.vector.(i).term) *: x.vector.(i).freq))
                    b;
                  array_iter_range 
                    (fun i x-> b.(i) <- b.(max (myxsize-1) 0)) b (myxsize, xsize-1);
                  let sum a b = 
                    if debug then assert((Array.length a)=(Array.length b));
                    Array.init (Array.length a) (fun i->a.(i) +: b.(i)) 
                  in   
                  let b_global = Parallel.hypercube_mnac_all b sum in
                    (*if profile then lprintf "|b_global|=%d\n" (Array.length b_global);*)
                    Array.iteri
                      (fun ix dvelt->
                         if debug then lprintf "checking term %d\n" dvelt.term;
                         if b_global.(ix) >=: t then (* NOTE: global pruning *)
                           MySV.append i.(dvelt.term) (x.docid, dvelt.freq) 
                         else
                           unindexed := (dvelt.term, dvelt.freq) :: !unindexed
                      ) x.vector;
                    vp.(x.docid) <- MyDV.of_list (List.rev !unindexed) x.docid x.cat;
                    comp_time := !comp_time +. (time () -. t_s)
           ) vblock;
           let t_s = time () in
             par_find_matches_1_minsize_vert_phase2_list i t !candidates o;
             tot_comm_time := !tot_comm_time +. (time () -. t_s);
      ) vblocks;
    if debug then
      (lprintf "\ni = \n"; 
       Array.iteri (fun n x -> lprintf "%d: " n; MySV.lprint x)
         i);
    if profile then 
      (lprintf "num cands = %d, comm_time = %g\n" !num_candidates !comm_time;
       lprintf "num scores = %d, comm_time2 = %g\n" !num_scores !comm_time2;
       lprintf "comp_time = %g, tot_comm_time =%g \n" !comp_time !tot_comm_time;
       lprintf "gather time = %g, barrier_time = %g, barrier_time2=%g, find_matches =%f\n"
         !Parallel.gather_time !Parallel.barrier_time !barrier_time2 !find_matches_time);
    o (* return output set*)
      
let par_all_pairs_0_local_matches v t =
  all_pairs_0_array v (t/.(float_of_int p))
      
let par_all_pairs_1_local_matches v t =
  all_pairs_1 v (t/.(float_of_int p))
      
let par_all_pairs_bf_local_matches v t =
  all_pairs_bruteforce v (t/.(float_of_int p))


(* feature-wise parallelization of all-pairs-bruteforce
   where each feature has a home processor
   v = list of document vectors, initially partitioned columnwise
       across processors
   t = threshold  
   with local pruning of candidates
*)

let all_pairs_bf_vert v t = 
  let t = Weight.of_float t in
  let n = List.length v in
  let logn = int_of_float ( Pervasives.log (float_of_int n) )  in 
  let o = Docmatch_Set.make_n (n * logn) in
  let a = Array.make n Weight.zero in 
  let va = Array.of_list v in
    if debug then lprintf "all-pairs-bf-vert\n";
    if profile then comm_time := 0.0;
    Array.iter MyDV.sort va;
    Array.iteri
      (fun i x ->
         if i mod 100=0 && pid=0 then 
           (printf "processing doc %d\r" i; flush stdout);
         if debug then MyDV.lprint x;
         for i=0 to n-1 do a.(i) <- Weight.zero done;
         for j = 0 to i-1 do
           let y = va.(j) in 
           let w = MyDV.dot_ordered x y in
             a.(j) <- w;
         done;
         merge_scores_array i a t o
      ) va;
    if profile then (lprintf "comm_time = %g\n" !comm_time);
    o (* return output set*)
