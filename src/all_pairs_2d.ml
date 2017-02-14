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
open Scanf
open Arg
open Gc

open Parallel
open AllPairs
open AllPairs2D
open Util

let dv_fname = ref ""
let threshold = ref 0.0
let q = ref 0
let r = ref 0

let algoix = ref None
let algoname = ref None
(*let partalgoix = ref None*)
let list_algos = ref false
let list_partalgos = ref false
let partalgoix = ref (Some 0)
let normalize = ref true

let speclist = 
  [
    ("-dv", String (fun x -> dv_fname := x), "document vector");
    ("-threshold", Float (fun x -> threshold := x), "similarity threshold");
    ("-q", Int (fun x -> q := x), "number of row blocks");
    ("-r", Int (fun x -> r := x), "number of column blocks");
    ("-algo", String (fun x -> algoix := Some (int_of_string x)), 
     "algorithm to run");
    ("-algoname ", String (fun x -> algoname := Some x), 
     "name of algorithm to run");
    ("-list", Unit (fun () -> list_algos := true), "list algorithms");
    ("-partalgo", String (fun x -> partalgoix := Some (int_of_string x)), 
     "partition algorithm");
    ("-listpart", Unit (fun () -> list_partalgos := true), "list partitioning algorithms");
    ("-nonorm", Unit (fun () -> normalize := false), "do not normalize")
  ]

let usage =  "Usage: all-pairs-2d -dv <dv-file> -threshold <sim-threshold> -q <num-rows> -r <num-cols> [-algo <algorithm>] [-algoname <algorithm-name>] [-list] [-partalgo <partition-algo>] [-listpart] [-nonorm]"

module FloatDV = Dv.Make (Types.FloatWeight)
module ConvertDV = Dv.Converter (FloatDV) (MyDV)


let algos = [
  (all_pairs_2d, "all-pairs-2d");
  (all_pairs_2d_opt, "all-pairs-2d-opt")
]

let part_algos = [
  (distribute_2d_cyclic, "vectors: cyclic, dims: cyclic");
  (distribute_2d_cyclic_scalar, "vectors: cyclic, dims: balance # scalar muls");
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
  if !q * !r != Parallel.p then  (
    if pid=0 then printf "Checkerboard args are incorrect\n%s\n" usage;
    exit 2;
  );
  AllPairs.adjust_gc (); 
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
      if pid=0 then printf "2D partitioning with: %s\n" name;
      algo fixdv_list !q !r in
  let algo_map = Hashtbl.create 10 in 

  let run (algo, name) =
    if pid = 0 then printf "\n* Running %s\n" name; flush stdout;
    let r = time_and_print_par 
      (fun ()-> algo mydv_list !threshold !q !r)
    and f = open_out (!dv_fname ^ "." ^ (string_of_int pid) ^ "." ^ name) in
    let loc_nmatches = AllPairs.Docmatch_Set.cardinal r in
    let nmatches = 
      Mpi.allreduce_int loc_nmatches Mpi.Int_sum Mpi.comm_world in
      lprintf "Local number of matches: %d\n" loc_nmatches;
      lprintf "Number of matches: %d\n" nmatches;
      if pid=0 then printf "Number of matches: %d\n" nmatches;
      AllPairs.Docmatch_Set.iter (fun (x,y,w) -> fprintf f "%d %d: %f\n" 
                                    x y (Weight.to_float w)) r;
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
