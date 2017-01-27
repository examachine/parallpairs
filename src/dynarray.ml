(*
**
** ocaml module Dynarray
**
** Description: Array with dynamic size
** Yes, this is the original Dynarray from 2002!
**
** Author: Eray Ozkural (exa) <examachine@gmail.com>, (C) 2002-2999
**
** Copyright: See COPYING file that comes with this distribution
**
*)

open Printf
open Util

type 'a dynarray = { mutable vec: 'a array; mutable size: int;
		     def: (int -> 'a) }

let make x = { vec = Array.make 1 x;
	       size = 0;
	       def = function _ -> x }

let init n f = { vec = Array.init n f;
                 size = n;
                 def = f}

let make_reserved x len = 
  { vec = Array.make len x;
    size = 0;
    def = function _ -> x }

let length a = a.size
let default a = a.def

let adjust_size a ix =
  if ix+1 > a.size then a.size <- ix+1 else ();
  if ix >= Array.length a.vec then
    let new_size = max (ix+1) (Array.length a.vec * 2) in
    let new_vec = Array.init new_size a.def in
      begin
	Array.blit a.vec 0 new_vec 0 (Array.length a.vec);
	a.vec <- new_vec;
      end
  else
    ()

let clear a = a.size <- 0

let get a ix =
  adjust_size a ix;
  a.vec.(ix)

let set a ix v =
  adjust_size a ix;
  a.vec.(ix) <- v

let append a v =
  adjust_size a (a.size);
  a.vec.(a.size-1) <- v

let extend (a: 'a  dynarray) (a2: 'a dynarray) =
  let oldsize = a.size in
    adjust_size a (a.size + a2.size - 1);
    Array.blit a2.vec 0 a.vec oldsize a2.size

let extend_vec (a: 'a  dynarray) (a2: 'a array) =
  let oldsize = a.size in
    adjust_size a (a.size + (Array.length a2) - 1);
    Array.blit a2 0 a.vec oldsize (Array.length a2)

let blit v1 o1 v2 o2 len = Array.blit v1.vec o1 v2.vec o2 len

let vec a = Array.sub a.vec 0 a.size

let subvec a x y = Array.sub a.vec x y

let mapa f a = Array.map f (vec a)

let mapai f a = Array.mapi f (vec a)

let iter f a = Array.iter f (vec a)

let iteri f a = Array.iteri f (vec a)

let iter_range f a (s,e) =
  for ix=s to e do
    f a.vec.(ix)
  done
    
let map_range f a (s,e) =  
  let m = make () in
    adjust_size m (e-s+1);
    for ix=s to e do
      m.vec.(ix-s) <- f a.vec.(ix)
    done;
    m

let sort f a = Array.sort f (vec a)

let print printelt l =
  printf "[| ";
  iter (fun x-> printelt x; printf "; ") l;
  printf " |]"
 
let print_int l = print print_int l

let print_float l = print print_float l



