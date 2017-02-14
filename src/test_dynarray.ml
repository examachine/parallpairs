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
 
let main () =
  printf "testing dynarray\n";
  let ay,ad = DynarrayPair.make 0 0 in
    Dynarray.append ay 3; Dynarray.append ad 8;
    Dynarray.append ay 5; Dynarray.append ad 2;
    Dynarray.append ay 1; Dynarray.append ad 4;
    let (ay', ad') = DynarrayPair.sort_first compare (ay,ad) in
      Dynarray.print_int ay'; Dynarray.print_int ad'
;;

main ()

  
