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

open Util
open Printf

let main () =
  Random.self_init ();
  let a1 = Array.init 10000001 (fun x->Random.int 15000) 
  and a2 = Array.init 10000001 (fun x->Random.float 1.0) in
  let x2 = ref 0.0 
  and x1 = ref 0 in
    time_and_print (fun ()->Array.iter (fun x-> x1 := !x1 + x*x) a1);
    time_and_print (fun ()->Array.iter (fun x-> x2 := !x2 +. x*.x) a2);
    time_and_print (fun () ->
                      for i=1 to  10000000 do
                        x1 := !x1 + (Util.min a1.(i-1) a1.(i));
                      done);
    time_and_print (fun () ->
                      for i=1 to  10000000 do
                        x1 := !x1 + (if a1.(i-1) <= a1.(i) then a1.(i-1) 
                                      else a1.(i));
                      done);
    time_and_print (fun () ->
                      for i=1 to  10000000 do
                        x2 := !x2 +. (Util.min a2.(i-1) a2.(i));
                      done);
    time_and_print (fun () ->
                      for i=1 to  10000000 do
                        x2 := !x2 +. (if a2.(i-1) <= a2.(i) then a2.(i-1) 
                                      else a2.(i));
                      done);
    printf "test done\n"
;;

main();;


                                  
