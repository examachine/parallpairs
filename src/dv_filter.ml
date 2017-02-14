(* 
**
** Normalize and then split a dataset into high-pass and low-pass
** subsets 
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
      
module FloatDV = Dv.Make (Types.FloatWeight)
module ConvertDV = Dv.Converter (FloatDV) (MyDV)

let in_fname = ref ""
let threshold = ref 0
let normalize = ref true

let speclist = 
  [
    ("-nonorm", Unit (fun () -> normalize := false), "do not normalize");
  ]

let usage =  "Usage: filter-dv <in-file> <threshold> [no-norm]"

let filter v s = 
  let m = 1 + max_dim v in 
  let dim_nz = calc_dim_nz v m 
  and order = reorder_dims v m in  
  let vsplit = Util.map (split_dv dim_nz s) v in
  let vh,vl = Util.split vsplit in
    vh, vl

let main () =
  if not ((Array.length Sys.argv)=3 || (Array.length Sys.argv)=4) then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  in_fname := Sys.argv.(1); 
  threshold := int_of_string Sys.argv.(2);
  if (Array.length Sys.argv)=4 then normalize := false;
  printf "Reading DV file %s\n" !in_fname; flush stdout;
  let floatdv_list = 
    if !normalize then (
      printf "Normalizing\n";
      Util.map FloatDV.normalize (FloatDV.read_dv !in_fname)
    )
    else 
      FloatDV.read_dv !in_fname in
  let fixdv_list = Util.map ConvertDV.convert floatdv_list in
  let prefix = String.sub !in_fname 0 ((String.length !in_fname)-3) in
    printf "Read %d vectors\n" (List.length fixdv_list);
    flush stdout;
    let dv_hp, dv_lp = filter fixdv_list !threshold in
    let hp_name = prefix ^ ".hp." ^ (string_of_int !threshold) ^ ".dv" 
    and lp_name = prefix ^ ".lp." ^ (string_of_int !threshold) ^ ".dv" in
      printf "Writing high-pass filtered DV file %s\n" hp_name; 
      flush stdout;
      MyDV.write_dv hp_name dv_hp;
      printf "Writing low-pass filtered DV file %s\n" lp_name; 
      flush stdout;
      MyDV.write_dv lp_name dv_lp;;

main();

