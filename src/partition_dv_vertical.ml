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
open AllPairs
open Util

module DV = Dv.Make (Types.FloatWeight)

let dv_fname = ref ""

let speclist = 
  [
    ("-dv", String (fun x -> dv_fname := x), "document vector file")
  ]

let usage =  "Usage: part-dv-colblock -dv <dv-file>"
 
let pid = Mpi.comm_rank Mpi.comm_world

 
let main () =
  Arg.parse speclist (fun x -> ()) usage;
  printf "Processor %d\n" pid;
  if !dv_fname="" then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  let dv_list =
    if pid = 0 then 
      (
        printf "Reading DV file %s\n" !dv_fname; flush stdout;
        DV.read_dv !dv_fname
        (*printf "Read %d vectors\n" (List.length dv_list); flush stdout;
          printf "Splitting DV %s\n" !dv_fname; flush stdout;*)
      )
    else
      [] in  
  let dv_list2 = ParAllPairs.distribute_dims_cyclic_dvlist dv_list in 
    DV.write_dv (!dv_fname ^ "." ^ (string_of_int pid)) dv_list2;;
    
main();


    
