(* 
**
** Sparse Vector abstract data type and two implementations of it
** using linked lists and dynamically sized arrays 
** TODO: array implementation?
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

module type SparseVectorSig =
sig
  (*module WeightType*)
  type weight
  type t
  val make: unit -> t (* make empty sparsevector *)
  val print: t -> unit (* print sparsevector *)
  val lprint: t -> unit (* print sparsevector to log file *)
  val iter:  ( (int * weight) -> unit ) -> t -> unit
  (*val map: ( (int * weight) -> 'a ) -> t -> 'b*)
  val append: t -> (int * weight) -> unit
  val front: t -> (int * weight)
  val pop: t -> unit
  val length: t -> int
end

module ListImpl (Weight: Types.WeightType) : SparseVectorSig
  with type weight = Weight.t
  =
struct

  type weight = Weight.t

  type t = ( int * weight ) list ref

  let make ()  = ref []

  let print sv = 
    
    (List.iter (fun (x,w)->Printf.printf "(%d, %f) " x 
                  (Weight.to_float w)) !sv;
     Printf.printf "\n")

  let lprint sv = 
    
    (List.iter (fun (x,w)->Printf.printf "(%d, %f) " x 
                  (Weight.to_float w)) !sv;
     Parallel.lprintf "\n")

(*let has sv ix =
  Dynarray.mapa
*)

  let iter f a = List.iter f !a
    
  (*let map f a = List.map f !a*) (* TODO: Util.map? *)

  let append a x = a := x::!a

  let front a = List.hd !a

  let pop a = a := List.tl !a

  let length a = List.length !a

end



module DynarrayImpl (Weight: Types.WeightType) : SparseVectorSig
  with type weight = Weight.t
  =
struct

  type weight = Weight.t

  type t = { da: (int * weight) Dynarray.dynarray; mutable start:int}

  let make ()  = 
    {da = Dynarray.make (0,Weight.zero); start = 0}

  let print a = 
    Dynarray.iter_range 
      (fun i (x,w)->Printf.printf "(%d, %f) " x (Weight.to_float w))
      a.da (a.start, (Dynarray.length a.da) - 1);
    Printf.printf "\n"

  let lprint a = 
    Dynarray.iter_range
      (fun i (x,w)->Parallel.lprintf "(%d, %f) " x (Weight.to_float w))
      a.da (a.start, (Dynarray.length a.da) - 1);
    Parallel.lprintf "\n"

  let iter f a = Dynarray.iter_range f a.da (a.start, (Dynarray.length a.da) - 1)
    
  (*let map f a = Dynarray.map f a.da*)
    
  let append a x = Dynarray.append a.da x

  let front a = Dynarray.get a.da a.start

  let pop a = a.start <- a.start + 1

  let length a = (Dynarray.length a.da) - a.start 

end
