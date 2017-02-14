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
open Util

let main () =
  let r = Util.range 2 10
  and r2 = Util.range 10 18 in
  let c = Util.combine r r2 in
  let m = Util.map (fun x->x*x) r in 
    print_intlist r; printf "\n";
    print_list print_intpair c; printf "\n";
    print_intlist m; printf "\n";
    print_intlist (Util.append r r2); printf "\n";
    print_intlist (Util.concat [r;r2;r2]); printf "\n";
    let x,y = Util.split_first_n 4 r in print_intlist x; print_intlist y; printf "\n";;

main();


    
