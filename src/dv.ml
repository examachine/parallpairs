(* 
**
** ocaml module Dv
**
** Description: support for Barla's DV (document vector) format
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
open Util
open Parallel

let debug = false

module type DV =
sig
  module MyWeight : Types.WeightType

  type weight = MyWeight.t

  type dvdata = {
    term: int;
    freq: weight
  }

  type dv = {
    docid: int; 
    vector: dvdata array;
    cat: int
  }

  val emptydvelt: dvdata
  val makedvelt: int -> weight -> dvdata
  val length: dv -> int
  val magnitude: dv -> weight
  val normalize: dv -> dv
  val make_emptydv: unit -> dv
  val make_dv: int -> dvdata array -> int -> dv 
  val of_list: (int * weight) list -> int -> int -> dv
  val print: dv -> unit
  val lprint: dv -> unit
  val read_dv: string -> dv list
  val write_dv: ?write_cat:bool -> string -> dv list -> unit
  val read_dv_svm: string -> dv list
  val write_dv_svm: string -> dv list -> unit
  val dot_ordered: dv -> dv -> weight
  val dot: dv -> dv -> int array -> weight
  val sort: dv -> unit (* sort in order of increasing terms in place *)
  (*val sub: dv -> dv -> dv
  val add: dv -> dv -> dv*)
end
  

module Make (Weight: Types.WeightType) : DV
  with module MyWeight = Weight 
  and type MyWeight.t = Weight.t =
struct
  module MyWeight = Weight

  type weight = Weight.t

  type dvdata = {
    term: int;
    freq: weight
  }

  type dv = {
    docid: int; 
    vector: dvdata array;
    cat: int
  }

  let emptydvelt = {term=0;freq=Weight.zero}

  let makedvelt t f = {term=t; freq=f}

  let length dv = Array.length dv.vector

  let magnitude dv = Weight.sqrt (Array.fold_left Weight.add Weight.zero
                                    (Array.map 
                                       (fun x->Weight.mul x.freq x.freq)
                                       dv.vector) )

  let normalize dv = 
    let mag = magnitude dv in 
      {docid=dv.docid;
       vector=Array.map (fun x->{term=x.term;freq=Weight.div x.freq mag}) 
          dv.vector;
       cat=dv.cat};;

  let sort dv = 
    Array.sort (fun d1 d2 -> compare d1.term d2.term) dv.vector

  let make_emptydv () =
    {docid=0;vector=Array.make 0 {term=0;freq=Weight.zero}; cat=0}

  let make_dv d v c = {docid=d; vector=v; cat=c}

  let of_list a d c= 
    match a with
        [] -> make_emptydv ()
      | list ->
          
          let a = Array.of_list list in 
            {docid=d;
             vector = Array.init (Array.length a) (fun ix->{term=fst a.(ix);
                                                            freq=snd a.(ix)});
             cat = c}
              
  let print dv = 
    printf "docid=%d " dv.docid;
    Array.iter (fun x->printf "(%d, %f) " x.term 
                  (Weight.to_float x.freq)) dv.vector;
    printf " cat=%d\n" dv.cat

  let lprint dv = 
    Parallel.lprintf "docid=%d " dv.docid;
    Array.iter (fun x->Parallel.lprintf "(%d, %f) " x.term 
                  (Weight.to_float x.freq)) dv.vector;
    Parallel.lprintf " cat=%d\n" dv.cat
      
  let read_dv fname = 
    let f = open_in fname in
    let lines = input_lines f in
    let dix = ref 0 in
    let process line =
      let n = ref 0 in
      let dcat = ref 0 in
      let buf = Scanning.from_string line in
        if debug then (printf "parsing <%s>\n" line; flush stdout);
        bscanf buf "%d " (fun x -> n:=x);
        let dvec = Array.init !n
	  ( fun i ->
	      let dvelt = ref emptydvelt in
	        bscanf buf "%d %f " (fun t f -> dvelt :=
				       {term=t;freq=Weight.of_float f});
	        !dvelt
	  ) in
          (try
	     bscanf buf "%d" (fun x -> dcat:=x)
           with End_of_file -> dcat := 0);
          dix := !dix + 1;
          if debug then printf "dix=%d\n" !dix;
	  {docid = !dix-1; vector = dvec; cat = !dcat}
    in
    let dv_list = Util.map process lines in
      close_in f;
      if debug then (printf "read dv "; List.iter print dv_list);
      dv_list

  let write_dv ?(write_cat=false) fname dv_list  = 
    let f = open_out fname in
    let write dv =
      fprintf f "%d" (Array.length dv.vector);
      Array.iter (fun dvelt -> fprintf f " %d %f" (dvelt.term) 
                    (Weight.to_float dvelt.freq))
        dv.vector;
      if write_cat then (fprintf f " %d" dv.cat);
      fprintf f "\n"
    in
      List.iter write dv_list;
      close_out f
        
  let read_dv_svm fname = 
    let f = open_in fname in
    let lines = input_lines f in
    let dix = ref 0 in
    let process line = 
      let dcat = ref 0 in
      let buf = Scanning.from_string line in
        if debug then (printf "parsing <%s>\n" line; flush stdout);
        bscanf buf "%d " (fun x -> dcat:=x);
        let l = ref [] in
          (try
             while true do
	       let dvelt = ref emptydvelt in
	         bscanf buf "%d:%f " (fun t f -> dvelt := {term=t;
                                                           freq=Weight.of_float f});
                 l := !dvelt :: !l
             done;
           with End_of_file -> ()); 
          let dvec = Array.of_list (List.rev !l) in
            dix := !dix + 1;
            if debug then printf "dix=%d\n" !dix;
	    {docid = !dix-1; vector = dvec; cat = !dcat}
    in
    let dv_list = Util.map process lines in
      close_in f;
      if debug then (printf "read dv "; List.iter print dv_list);
      dv_list

  let write_dv_svm fname dv_list =
    let f = open_out fname in
    let write dv =
      fprintf f "%d" dv.cat;
      Array.iter (fun dvelt -> fprintf f " %d:%f" (dvelt.term+1) 
                    (Weight.to_float dvelt.freq))
        dv.vector;
      fprintf f "\n"
    in
      List.iter write dv_list;
      close_out f

  (* assume terms of a and b are in ascending order *)
  let dot_ordered a b =
    let s = ref Weight.zero
    and aix = ref 0
    and bix = ref 0 
    and alength = Array.length a.vector 
    and blength = Array.length b.vector in
      if debug then (printf "dot "; print a; print b);
      while (!aix < alength && !bix < blength) do
        let comp = compare a.vector.(!aix).term b.vector.(!bix).term in
          if comp < 0 then
            aix := !aix + 1
          else 
            if comp = 0 then
              (s := Weight.add !s (Weight.mul a.vector.(!aix).freq  b.vector.(!bix).freq);
               aix := !aix + 1;
               bix := !bix + 1)
            else
              bix := !bix + 1
      done;
      if debug then printf "s = %f\n" (Weight.to_float !s);
      !s


  (* TODO: use binary search *)
  let dot a b order =
    let s = ref Weight.zero
    and aix = ref 0
    and bix = ref 0 
    and alength = Array.length a.vector 
    and blength = Array.length b.vector in
      if debug then (printf "dot "; print a; print b);
      while (!aix < alength && !bix < blength) do
        let comp = compare order.(a.vector.(!aix).term) 
          order.(b.vector.(!bix).term) in
          if comp < 0 then
            aix := !aix + 1
          else 
            if comp = 0 then
              (let p = Weight.mul a.vector.(!aix).freq  b.vector.(!bix).freq
               in s := Weight.add !s p;
                 (*if debug then lprintf "%f \n" (Weight.to_float p);*)
                 aix := !aix + 1;
                 bix := !bix + 1)
            else
              bix := !bix + 1
      done;
      if debug then printf "s = %f\n" (Weight.to_float !s);
      !s

 (* subtract two vectors 
    TODO: why doesnt the same code work for SparseVector? 
  let sub a b =
    let r = a 
    and aix = ref 0
    and bix = ref 0 
    and alength = Array.length a.vector 
    and blength = Array.length b.vector in
      if debug then (printf "sub "; print a; print b);
      Array.sort (fun x y -> compare x.term y.term) a.vector;
      Array.sort (fun x y -> compare x.term y.term) b.vector;
      while (!aix < alength && !bix < blength) do
        let comp = compare a.vector.(!aix).term b.vector.(!bix).term in
          if comp < 0 then
            aix := !aix + 1
          else 
            if comp = 0 then
	      (r.vector(!aix).freq <- 
		 Weight.sub a.vector.(!aix).freq b.vector.(!bix).freq; 
	       aix := !aix + 1;
	       bix := !bix + 1)
            else
	      bix := !bix + 1
      done;
      r*)

end

module Converter (Dv1: DV) (Dv2: DV) :
  sig
    val convert: Dv1.dv -> Dv2.dv
  end =
struct
  open Dv1

  let convert dv1 =
    
    {
      Dv2.docid = dv1.docid ;
      Dv2.vector = 
        Array.map (fun dvelt ->
                     {
                       Dv2.term = dvelt.term;
                       Dv2.freq = Dv2.MyWeight.of_float
                         (Dv1.MyWeight.to_float dvelt.freq) 
                     }
                  ) dv1.vector;
      Dv2.cat = dv1.cat
    }

end
    
