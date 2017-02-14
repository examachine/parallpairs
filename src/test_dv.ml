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

module DV = Dv.Make (Types.FloatWeight)


let main () =
  let x = DV.emptydvelt in
    printf "testing dv\n";
    for docid=0 to 10 do
      printf "doc %d\n" docid;
      let dv_list = List.map 
        (fun i-> (Random.int 1000,
                  Types.FloatWeight.of_float (Random.float 1.0)) )
        (Util.range 1 10) in
        DV.print (DV.of_list dv_list 0 0);
        printf "\n";
    done;;
      
main ();;

