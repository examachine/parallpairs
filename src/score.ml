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

open Types
open Util
open Parallel

module Weight = Types.Fix32Weight
open Weight
 
let debug = false

let lprint_assoc_list al name =
  lprintf "\n%s: " name;
  lprint_list (fun (x,y) -> lprintf "(%d,%f)" x (Weight.to_float y)) al;
  lprintf "\n"

(* merge two association lists a and b
   precond: a and b are sorted in increasing order of keys *)
let merge_als (a: (int*Weight.t) list) b =
  let rec loop a b (acc: (int*Weight.t) list) =
    match a with 
        [] -> Util.append (List.rev b) acc 
      | (akey,avalue)::atl ->
          (*lprintf "ahd=%d,%g\n" akey (Weight.to_float avalue);*)
          match b with 
              [] -> Util.append (List.rev a) acc 
            | (bkey,bvalue)::btl ->
                (*lprintf "bhd=%d,%g\n" bkey (Weight.to_float bvalue);*)
                if akey < bkey then 
                  loop atl b ((akey,avalue)::acc)
                else 
                  if akey = bkey then  
                    loop atl btl ((akey,avalue+:bvalue)::acc)
                  else
                    loop a btl ((bkey,bvalue)::acc)
  in
    List.rev (loop a b [])


let sort_al al =
  List.sort (fun (k,_) (k2,_)-> compare k k2) al


(* Tail recursive list processing algorithm for
   merging the consecutive associative pairs with identical keys
   by adding their values *)
let merge_duplicates l =
  let rec loop a prevkey acc =
    (*lprint_assoc_list a "loop";
    lprint_assoc_list acc "acc";*)
    match a with
      [] -> acc
    | (key,weight)::tl -> 
        if key = prevkey then
          let _,prevweight = List.hd acc in
            loop tl key ( (key, prevweight+:weight) :: (List.tl acc) )
        else
          loop tl key ( (key,weight) :: acc)
  in
    loop (List.rev l) (-1) []
      

(* Merge association lists 
   al_list is a list of al's where al is a list of int, float weights
*)

let merge_assoc_lists al_list =
  let sort al = 
    List.sort (fun (x1,w1) (x2,w2) -> compare x1 x2) al in    
  let sorted = sort (Util.concat al_list) in
    if debug then lprint_assoc_list sorted "sorted";
    (* merge consecutive duplicate items now *)
    merge_duplicates sorted
      (* swap objects between two processors 
         src dest are processors to perform the swap
      *)


(*TODO: merge n sorted lists
  let merge_n_sets set_list = 
  let sorted = List.sort compare (Util.concat set) in
*)


(* union of two sets represented as sorted arrays a and b
   precond: a and b are sorted in increasing order  *)
let merge_dynarray_sets a b =
  let na = Dynarray.length a and nb = Dynarray.length b in
  let u = Dynarray.make_reserved 0 (max na nb) in
  let i = ref 0 and j = ref 0 and complete = ref false in
    while not !complete do
      if !i < na && !j < nb then 
        (let ai = Dynarray.get a !i and bj = Dynarray.get b !j in
           if debug then 
             (printf "i=%d, ai=%d, j=%d, bj=%d " !i ai !j bj;
              Dynarray.print_int u; print_newline ());
        if ai < bj then
          (Dynarray.append u ai; incr i)
        else 
          if ai = bj then 
            (Dynarray.append u ai; incr i; incr j)
          else
            (Dynarray.append u bj; incr j));
      if !i = na then 
        ( Dynarray.extend_vec u (Dynarray.subvec b !j (nb- !j));
          complete := true)
      else if !j = nb then 
        (Dynarray.extend_vec u (Dynarray.subvec a !i (na- !i));
           complete := true)
    done;
    u

(* union of two score maps represented as sorted array pairs (ay,aw) and 
   (by,bw)
   precond: (ay,aw) and (by,bw) are sorted in increasing order of vec ids *)
let merge_dynarray_pair_sets (ay,aw) (by,bw) =
  let na = Dynarray.length ay and nb = Dynarray.length by in
  let uy,uw = 
    DynarrayPair.make_reserved 0 (max na nb) Weight.zero (max na nb) in
  let u = (uy,uw) in
  let i = ref 0 and j = ref 0 and complete = ref false in
    while not !complete do
      if !i < na && !j < nb then 
      (let ayi = Dynarray.get ay !i and byj = Dynarray.get by !j in
       let awi = Dynarray.get aw !i and bwj = Dynarray.get bw !j in
         if ayi < byj then
           (DynarrayPair.append u ayi awi; incr i)
         else 
           if ayi = byj then 
             (DynarrayPair.append u ayi (awi+bwj); incr i; incr j)
           else
             (DynarrayPair.append u byj bwj; incr j));
      if !i = na then 
        ( Dynarray.extend_vec uy (Dynarray.subvec by !j (nb- !j));
          Dynarray.extend_vec uw (Dynarray.subvec bw !j (nb- !j));
          complete := true);
      if !j = nb then 
        (Dynarray.extend_vec uy (Dynarray.subvec ay !i (na- !i));
         Dynarray.extend_vec uw (Dynarray.subvec aw !i (na- !i));
         complete := true)
    done;
    u
