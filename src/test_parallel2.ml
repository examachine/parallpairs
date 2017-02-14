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

module Weight = Types.Fix32Weight
open Weight


let p = Mpi.comm_size Mpi.comm_world

let pid = Mpi.comm_rank Mpi.comm_world

let main () =
    
  Random.self_init ();
  lprintf "test-parallel, mpi pid=%d, system pid=%d\n" pid (Unix.getpid ());
 
  (
    lprintf "testing exchange of 4 million ints\n";
    let a = Array.init 4000000 (fun x->Random.int 100) in
    let t0 = new timer in
    let x = exchange a pid (pid lxor 1) in 
      lprintf "|x|=%d " (Array.length x);
      lprintf "exchange time=%g\n" t0#elapsed;
  );
  (
    lprintf "testing exchange of 4 million ints\n";
    let a = Array.init 4000000 (fun x->Random.int 100) in
    let t0 = new timer in
    let x = exchange_int_array a pid (pid lxor 1) in 
      lprintf "|x|=%d " (Array.length x);
      lprintf "exchange time=%g\n" t0#elapsed;
  );
  (
    lprintf "testing exchange of 100000 40-length vectors\n";
    let a = Array.init 40 (fun x->Random.int 100) in
    let t0 = new timer in
    let n = ref 0 in
      for i=0 to 100000 do
        let x = exchange a pid (pid lxor 1) in 
          n := !n + (Array.length x);
      done;
      lprintf "exchange time=%g\n" t0#elapsed;
  )
;;

main ();
