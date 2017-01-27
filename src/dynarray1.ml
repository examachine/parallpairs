(*
**
** ocaml module Dynarray
**
** Description: Array with dynamic size
** Yes, this is the original Dynarray from 2002!
** Modified to use Bigarray.Array1
**
** Author: Eray Ozkural (exa) <examachine@gmail.com>, (C) 2002-2999
**
** Copyright: See COPYING file that comes with this distribution
**
*)

open Bigarray

(* 'kind must be a bigarray kind *)
type ('a,'b) dynarray = 
    { mutable vec: ('a, 'b, c_layout) Array1.t; 
      mutable size: int;
      kind: ('a,'b) kind }

let make kind = { vec = Array1.create kind c_layout 1;
	          size = 0; kind=kind }

let make_reserved kind len = 
  { vec = Array1.create kind c_layout len;
    size = 0; kind=kind }

let length a = a.size

let adjust_size a ix =
  if ix+1 > a.size then a.size <- ix+1 else ();
  if ix >= Array1.dim a.vec then
    let new_size = max (ix+1) (Array1.dim a.vec * 2) in
    let new_vec = Array1.create a.kind c_layout new_size in
      begin
	Array1.blit a.vec (Array1.sub new_vec 0 (Array1.dim a.vec));
	a.vec <- new_vec;
      end
  else
    ()

let clear a = a.size <- 0

let get a ix =
  adjust_size a ix;
  a.vec.{ix}

let set a ix v =
  adjust_size a ix;
  a.vec.{ix} <- v

let append a v =
  adjust_size a (a.size);
  a.vec.{a.size-1} <- v

let vec a = Array1.sub a.vec 0 a.size

let extend a a2 =
  let oldsize = a.size in
    adjust_size a (a.size + a2.size - 1);
    Array1.blit (vec a2) (Array1.sub a.vec oldsize a2.size)

(*
let mapa f a = Array.map f (vec a)

let mapai f a = Array.mapi f (vec a)*)

let iter f a = 
  for i=0 to a.size-1 do
    f a
  done

let iteri f a = 
  for i=0 to a.size-1 do
    f i a
  done

  (*
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
*)
