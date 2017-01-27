open Printf
open Scanf
open Arg
open Gc

open Parallel
open AllPairs
open ParAllPairsVert
open AllPairs2D
open Util

let dv_fname = ref ""
let threshold = ref 0.0

let algoix = ref None
let algoname = ref None
(*let partalgoix = ref None*)
let list_algos = ref false
let list_partalgos = ref false
let partalgoix = ref (Some 0)
let normalize = ref true
let blocksize = ref 64

let speclist = 
  [
    ("-dv", String (fun x -> dv_fname := x), "document vector");
    ("-threshold", Float (fun x -> threshold := x), "similarity threshold");
    ("-algo", String (fun x -> algoix := Some (int_of_string x)), 
     "algorithm to run");
    ("-algoname ", String (fun x -> algoname := Some x), 
     "name of algorithm to run");
    ("-list", Unit (fun () -> list_algos := true), "list algorithms");
    ("-partalgo", String (fun x -> partalgoix := Some (int_of_string x)), 
     "partition algorithm");
    ("-listpart", Unit (fun () -> list_partalgos := true), "list partitioning algorithms");
    ("-nonorm", Unit (fun () -> normalize := false), "do not normalize");
    ("-blocksize", Int (fun x -> blocksize := x), "vector block size for vertical algorithm");
  ]

let usage =  "Usage: par-all-pairs -dv <dv-file> -threshold <sim-threshold> [-algo <algorithm>] [-algoname <algorithm-nameK>] [-list]"

module FloatDV = Dv.Make (Types.FloatWeight)
module ConvertDV = Dv.Converter (FloatDV) (MyDV)

let algos = [
  (par_all_pairs_0_vert, "par-all-pairs-0-vert");
  (par_all_pairs_0_array_vert, "par-all-pairs-0-array-vert");
  (par_all_pairs_0_array_vert_aapc, "par-all-pairs-0-array-vert-aapc");
  (par_all_pairs_0_array_vert_rec, "par-all-pairs-0-array-vert-rec");
  (par_all_pairs_0_array_vert_opt, "par-all-pairs-0-array-vert-opt");
  (par_all_pairs_0_array_vert_opt_pair, "par-all-pairs-0-array-vert-opt_pair");
  (par_all_pairs_0_array_vert_opt_mt, "par-all-pairs-0-array-vert-opt-mt");
  (all_pairs_bf_vert, "par-all-pairs-bf-vert");
  (par_all_pairs_0_array_vert_nopruning, "par-all-pairs-0-array-vert-nopruning");

  (*
  (par_all_pairs_0_vert_dense2, "par-all-pairs-0-vert-dense2");
  (par_all_pairs_0_minsize, "par-all-pairs-0-minsize");
  (par_all_pairs_1_vert, "par-all-pairs-1-vert");
  (*par_all_pairs_1_vert_dense, "par-all-pairs-1-vert-dense");*)
  (par_all_pairs_1_vert_local_pruning, "par-all-pairs-1-vert-local-pruning");
  (par_all_pairs_1_minsize_vert, "par-all-pairs-1-minsize-vert");
  (par_all_pairs_1_minsize_vert2, "par-all-pairs-1-minsize-vert2");
  (par_all_pairs_1_minsize_vert3, "par-all-pairs-1-minsize-vert3");*)
  (*(par_all_pairs_0_local_matches, "par-all-pairs-0-local-matches");*) 
  (*(par_all_pairs_1_local_matches, "par-all-pairs-1-local-matches");
    (par_all_pairs_bf_local_matches, "par-all-pairs-bf-local-matches");*)
]

let part_algos = [
  (distribute_dims_allpairs0_dvlist, "weight: allpairs0 sum_i C(freq_i,2)");
  (distribute_dims_cyclic_dvlist, "cyclic");
  (distribute_dims_decr_dens_cyclic_dvlist, "decreasing density order, cyclic");
  (distribute_dims_num_nz_dvlist, "weight: number of nonzeroes");
  (distribute_dims_allpairs_powerlaw_dvlist, "allpairs 0 powerlaw");
  (distribute_dims_allpairs1_dvlist, "weight: allpairs1 load")
]

let main () =
  Arg.parse speclist (fun x -> ()) usage;
  if !list_algos then
    (list_iteri (fun i (_,name) -> 
                  if pid=0 then printf "%d: %s\n" i name) algos; exit 0);
  if !list_partalgos then
    (list_iteri (fun i (_,name) -> 
                  if pid=0 then printf "%d: %s\n" i name) part_algos; exit 0);
  if (!dv_fname="" or !threshold=0.0) then (
    if pid=0 then printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  AllPairs.adjust_gc ();
  ParAllPairsVert.blocksize := !blocksize; (* lame global use *)
  let fixdv_list = 
    if pid = 0 then (
      printf "Reading DV file %s\n" !dv_fname; flush stdout;
      let floatdv_list = 
        if !normalize then (
          printf "Normalizing\n";
          Util.map FloatDV.normalize (FloatDV.read_dv !dv_fname)
        )
        else 
          FloatDV.read_dv !dv_fname
      in
        printf "Converting to Fixed Point\n"; flush stdout;
        Util.map ConvertDV.convert floatdv_list;
    )
    else [] in
  let mydv_list = 
    let algo, name = (List.nth part_algos (get_option !partalgoix)) in
      if pid=0 then printf "Partitioning dims with: %s\n" name;
      algo fixdv_list (Weight.of_float !threshold) in
  let algo_map = Hashtbl.create 10 in 
  let run (algo, name) =
    if pid = 0 then printf "\n* Running %s\n" name; flush stdout;
    let r = time_and_print_par (fun ()->algo mydv_list !threshold)
    and f = open_out (!dv_fname ^ "." ^ (string_of_int pid) ^ "." ^ name) in
    let loc_nmatches = AllPairs.Docmatch_Set.cardinal r in
    let nmatches = Mpi.allreduce_int loc_nmatches Mpi.Int_sum Mpi.comm_world in
      lprintf "Local number of matches: %d\n" loc_nmatches;
      lprintf "Number of matches: %d\n" nmatches;
      if pid=0 then printf "Number of matches: %d\n" nmatches;
      AllPairs.Docmatch_Set.iter (fun (x,y,w) -> fprintf f "%d %d: %f\n" 
                                    x y (Weight.to_float w)) r;
      if ParAllPairsVert.profile then 
        (lprintf "num cands=%d, comm_time=%g\n" !num_candidates !comm_time;
         lprintf "num scores=%d, comm_time2=%g\n" !num_scores  !comm_time2;
         lprintf "my scores=%d, comp_time=%g\n" !my_scores !comp_time;
         lprintf "gather time=%g, barrier_time=%g, find_matches =%f\n"
           !Parallel.gather_time !Parallel.barrier_time !find_matches_time;
         let avg x = 
           if x = 0.0 then
             0.0
           else
             x /. (float_of_int p) in
         
         let avg_comm = avg (Mpi.allreduce_float (!comm_time +. !comm_time2) Mpi.Float_sum Mpi.comm_world)  in
        let max_comm = Mpi.allreduce_float (!comm_time +. !comm_time2) Mpi.Float_max Mpi.comm_world in
         let avg_comp = avg (Mpi.allreduce_float !comp_time Mpi.Float_sum Mpi.comm_world)  in
        let max_comp = Mpi.allreduce_float !comp_time Mpi.Float_max Mpi.comm_world in
         let avg_cand = avg (float_of_int (Mpi.allreduce_int !num_candidates Mpi.Int_sum Mpi.comm_world)) in
         let max_cand = Mpi.allreduce_int !num_candidates Mpi.Int_max Mpi.comm_world in
         let gscores = Mpi.allreduce_int !num_scores Mpi.Int_sum Mpi.comm_world in
         let avg_barrier = avg (Mpi.allreduce_float !Parallel.barrier_time Mpi.Float_sum Mpi.comm_world)  in
         let max_barrier = Mpi.allreduce_float !Parallel.barrier_time Mpi.Float_max Mpi.comm_world in 
         if pid = 0 then (
           printf "prof: %g %g %g %g %d %g %d %g %g\n" avg_comm max_comm avg_comp max_comp !num_scores avg_cand max_cand avg_barrier max_barrier;
           printf "legend avgcomm maxcomm avgcomp maxcomp numscores avgcand maxcand avgbarrier maxbarrier\n"
         );
         (*lprint_gc_stats ()*));
      close_out f
  in
    List.iter (fun (algo, name) -> Hashtbl.add algo_map name algo) algos;
    if !algoix!=None then
      run (List.nth algos (get_option !algoix))
    else
      if !algoname!=None then
        let name = get_option !algoname in
          run (Hashtbl.find algo_map name, name)
        else
      List.iter (fun (algo, name) -> run (algo, name)) algos;;

main ();
