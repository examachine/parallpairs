open Printf
open Scanf
open Arg
open Gc

open Parallel
open ParAllPairsHoriz
open AllPairs2D
open Util

let dv_fname = ref ""
let threshold = ref 0.0

type algotype = int option

let algoix = ref None
let whichalgo aix = match aix with
    None -> -1
  | Some x -> x

let list_algos = ref false
let part_file = ref ""
let list_partalgos = ref false
let partalgoix = ref (Some 0)

let plainalgo f = fun v->f v !threshold

let algos = [
  (plainalgo par_all_pairs_0_horiz, "par-all-pairs-0-horiz");
  (plainalgo par_all_pairs_0_array_horiz, "par-all-pairs-0-array-horiz");
  (plainalgo par_all_pairs_minsize_horiz, "par-all-pairs-0-minsize-horiz");
  ( (fun v->par_all_pairs_0_array_horiz_rownet v !threshold
       !part_file), "par-all-pairs-0-array-horiz-rownet" ) ;
  (plainalgo par_all_pairs_1_horiz, "par-all-pairs-1-horiz");
  (plainalgo par_all_pairs_1_minsize_horiz, "par-all-pairs-1-minsize-horiz");
  (plainalgo all_pairs_bf_horiz, "par-all-pairs-bf-horiz");
]

let part_algos = [
  (distribute_vecs_cyclic, "cyclic");
]


let speclist = 
  [
    ("-dv", String (fun x -> dv_fname := x), "document vector");
    ("-threshold", Float (fun x -> threshold := x), "similarity threshold");
    ("-algo", String (fun x -> algoix := Some (int_of_string x)), 
     "algorithm to run");
    ("-list", Unit (fun () -> list_algos := true), "list algorithms");
    ("-partfile", String (fun x -> part_file := x), "row-net model partition vector");
    ("-partalgo", String (fun x -> partalgoix := Some (int_of_string x)), 
     "partition algorithm");
    ("-listpart", Unit (fun () -> list_partalgos := true), "list partitioning algorithms");
  ]

let usage =  "Usage: par-all-pairs-horiz -dv <dv-file> -threshold <sim-threshold> [-algo <algorithm>] [-list] [-partfile <partvec>] [-partalgo <algorithm>] [-listpart]"

module FloatDV = Dv.Make (Types.FloatWeight)
module ConvertDV = Dv.Converter (FloatDV) (MyDV)

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
  let fixdv_list = 
    if pid = 0 then (
        printf "Reading DV file %s\n" !dv_fname; flush stdout;
        let floatdv_list = Util.map FloatDV.normalize 
          (FloatDV.read_dv !dv_fname) in
          printf "Read file, distributing vectors in cyclic fashion\n";
          flush stdout;
          Util.map ConvertDV.convert floatdv_list
    )
    else
      [] in
  let mydv_list = 
    let algo, name = (List.nth part_algos (get_option !partalgoix)) in
      if pid=0 then printf "Partitioning vecs with: %s\n" name;
      algo fixdv_list (Weight.of_float !threshold) in
  let run (algo, name) =
    if pid = 0 then printf "\n* Running %s\n" name; flush stdout;
    let r = time_and_print_par (fun ()->algo mydv_list)
    and f = open_out (!dv_fname ^ "." ^ (string_of_int pid) ^ "." ^ name) in
    let loc_nmatches = AllPairs.Docmatch_Set.cardinal r in
    let nmatches = Mpi.allreduce_int loc_nmatches Mpi.Int_sum Mpi.comm_world in
      lprintf "Number of local matches: %d\n" loc_nmatches;
      lprintf "Number of matches: %d\n" nmatches;
      if pid=0 then printf "Number of matches: %d\n" nmatches;
      AllPairs.Docmatch_Set.iter (fun (x,y,w) -> fprintf f "%d %d: %f\n" 
                                    x y (Weight.to_float w)) r;
      close_out f;
      Mpi.barrier Mpi.comm_world in
    lprintf "distribution done, mysize=%d\n" (List.length mydv_list);
    if !algoix!=None then
      run (List.nth algos (whichalgo !algoix))
    else
      List.iter 
        (fun (algo, name) -> run (algo, name)) algos;;

main();
