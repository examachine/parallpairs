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

module Fix32 = Types.Fix32Weight

let main () =
  let a = Fix32.of_float 0.2 
  and b = Fix32.of_float 0.3 in
  let c = Fix32.mul a b in
    printf "a=%f, b=%f, a*b=%f\n"
      (Fix32.to_float a) (Fix32.to_float b) (Fix32.to_float c);;

main ()

