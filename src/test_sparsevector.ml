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

module SV = SparseVector.SparseVectorListImpl (Types.FloatWeight)


module SV2 = SparseVector.SparseVectorDynArrayImpl (Types.FloatWeight)

let main () =
  printf "testing sparsevector\n";
  let sv = SV.make () in
    SV.append sv (3,0.8);
    SV.append sv (5,0.2);
    SV.print sv;
  let sv = SV2.make () in
    SV2.append sv (3,0.8);
    SV2.append sv (5,0.2);
    SV2.print sv;
    
;;

main ()

  
