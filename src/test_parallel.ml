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

open Parallel
open Util
open Score
open ScoreAccum

module Weight = Types.Fix32Weight
open Weight


let p = Mpi.comm_size Mpi.comm_world

let pid = Mpi.comm_rank Mpi.comm_world

let test_aapc = 
  lprintf "test AAPC here\n"; 
  lprintf "for 4 processors\n";
  assert(p=4);
  let a = Array.make p [] in 
    (match pid with
        0 ->  
          a.(0) <- [0];
          a.(1) <- [2];
          a.(2) <- [3;0;2];
          a.(3) <- [4;1]
      | 1 ->  
          a.(0) <- [1;2];
          a.(1) <- [1;1];
          a.(2) <- [5;6];
          a.(3) <- [6;8;10] 
      | 2 ->
          a.(0) <- [0;0;0]; 
          a.(1) <- [1;0]; 
          a.(2) <- [2;2;2];
          a.(3) <- [2;3;4;5] 
      | 3 ->
          a.(0) <- [5;1;0];
          a.(1) <- [4;10;7;3]; 
          a.(2) <- [3;0;1;4;2;5;2];
          a.(3) <- [3;3;3]
      | n -> failwith "pid mismatch");
    lprintf "a = "; 
    lprint_array lprint_intlist a; lprintf "\n";
    let b = aapc a in 
      lprintf "b = "; 
      lprint_array lprint_intlist b; lprintf "\n" 

let main () =
    
  Random.self_init ();
  lprintf "test-parallel, mpi pid=%d, system pid=%d\n" pid (Unix.getpid ());
  test_aapc;
    (
      lprintf "testing exchange";
      let a = Random.int 10 in
      let x = exchange a pid (pid lxor 1) in
        lprintf "a=%d, partner's a is %d\n" a x
    );
    (
      lprintf "testing hypercube quicksort\n";
      let a = create_list (fun i->Random.int 10) 5 in
      let s = hypercube_quicksort a in
        lprintf "\na="; lprint_intlist a;lprintf "\n";
        lprintf "s="; lprint_intlist s;lprintf "\n";
    );
    (lprintf "testing naive parallel merge\n";
     let make_keyval _ = (Random.int 10, 
                          Weight.of_float (Random.float 1.0)) in
     let al = create_list make_keyval 5 in
       lprintf "al at proc. %d: " pid;
       lprint_list (fun (x,y) -> lprintf "(%d,%f)" x (Weight.to_float y)) al;
       (let m = par_merge_assoc_lists al in
          lprintf "\nm: ";
          lprint_list (fun (x,y) -> lprintf "(%d,%f)" x (Weight.to_float y)) m;
          lprintf "\n");
       (
         lprintf "testing mnac_all\n";
         let m = 
           hypercube_mnac_all al (fun x y->merge_assoc_lists [x;y]) in
           lprintf "\nm: ";
           lprint_list (fun (x,y) -> lprintf "(%d,%f)" x (Weight.to_float y)) m;
           lprintf "\n"
       );
       (
         lprintf "testing hypercube score accumulation\n";
         let m = hypercube_accumulate_scores al in
           lprintf "\nm: ";
           lprint_list (fun (x,y) -> lprintf "(%d,%f)" x (Weight.to_float y)) m;
           lprintf "\n"
       )
    );
;;

main ();
