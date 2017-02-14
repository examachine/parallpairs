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

open Unix
open Printf

let debug = false

(* module instantiations *)

module Int = struct 
  type t = int 
  let compare = compare 
  let hash = Hashtbl.hash 
  let equal = (=)
  let default = 0
end

module IntSet = Set.Make(Int)

let intset_of_list l = 
  List.fold_left (fun s x -> IntSet.add x s) IntSet.empty l

let print_intset (a : IntSet.t) =
  printf "{ ";
  IntSet.iter (fun x-> printf "%d, " x) a;
  printf " }"

(* types for the graph library *)
 
module Float = struct 
  type t = float 
  let compare = compare 
  let hash = Hashtbl.hash 
  let equal = (=)
  let default = 0.0
end


module FloatRef = struct 
  type t = float ref 
  let compare = compare 
  let hash = Hashtbl.hash 
  let equal = (=)
  let default = ref 0.0
end


(* Misc. utilities *)

let logb x b = log x /. log b

let log2 x = logb x 2.0

let log2_int x = int_of_float (log2 (float_of_int x))

(* is given number a power of two? *)
let is_pow2 x = floor (log2 x) = log2 x

(* compute random permutation of an array of indices from 0 to n-1 *)
let random_permute n =
  let a = Array.init n (function i -> i)
  and swap a i j = let t = a.(i) in a.(i) <- a.(j); a.(j) <- t
  in
    Random.self_init ();
    for i=1 to n-1 do
      swap a (Random.int i) i
    done;
    a

(* choose an array element at random *)
let choose_random a = Array.get a (Random.int (Array.length a))

(*BUG: not tail recursive *)
let input_lines file =
  let rec input_aux file =
    try
      let line = input_line file in
      let tail = input_aux file in
	line :: tail
    with End_of_file -> []
  in
    input_aux file

let input_lines file =
  let out = ref [] in
    (try 
       while true do
         out := (input_line file) :: !out
       done
    with End_of_file -> ());
    List.rev !out
      
let read_file fname =
  let f = open_in fname in
  let n = in_channel_length f in
  let buf = String.create n in
    really_input f buf 0 n;
    close_in f;
    buf
      
let read_directory dir =
  let rec read_dir_aux h =
    try
      let head = readdir h in
      let tail = read_dir_aux h in
	head::tail
    with End_of_file -> []
  in
    read_dir_aux (opendir dir)

(* remove . and .., then sort entries *)
let read_dir_files dir =
  let entries = read_directory dir in
  let notdir s = (s <> ".") && (s <> "..") in
    List.sort String.compare (List.filter notdir entries)

(* given n integers a, compute
s_0 = 0
s_k_{k=1 to n}  = \sigma_i=1^{i=k} a_i *)
let sum_from_start a =
  let n = Array.length a in
  let s = Array.make (n+1) 0 in
    s.(0) <- 0;
    for i = 1 to n do
      s.(i) <- s.(i-1) + a.(i-1)
    done;
    s

(* tail recursive range function *)
let range a b =
  let rec loop a b acc =
    if a > b then acc
    else loop (a+1) b (a :: acc)
  in
    List.rev (loop a b [])

(* repeat an element *)
let repeat a n =
  let rec loop n acc =
    if n = 0 then acc
    else loop (n-1) (a :: acc)
  in
    loop n []

(* construct a list where f () produces an element *)
let construct f n =
  let rec loop n acc =
    if n = 0 then acc
    else loop (n-1) (f () :: acc)
  in
    loop n []

(* tail recursive range function *)
let combine a b =
  let rec loop a b acc =
    if a=[] then acc
    else loop (List.tl a) (List.tl b) ( (List.hd a, List.hd b) :: acc)
  in
    List.rev (loop a b [])

(* tail recursive map function *)
let map f a =
  let rec loop a acc =
    if a=[] then acc
    else loop (List.tl a) ( (f (List.hd a)) :: acc)
  in
    List.rev (loop a [])

let map2 f a b =
  let rec loop a b acc =
    if a=[] then acc
    else loop (List.tl a) (List.tl b) ( (f (List.hd a) (List.hd b)) :: acc)
  in
    List.rev (loop a b [])

let array_map2 f a b =
  let n = Array.length a in
    assert(n=Array.length b);
    Array.init n (fun ix->f a.(ix) b.(ix))


let split a = (map (fun (x,y) -> x) a, map (fun (x,y) -> y) a)

(* partition a list into two parts according to a given condition [cond]*)
let partition cond a =
  let rec loop a acc =
    match a with 
        [] -> acc
      | hd::tl ->
          if cond hd then 
            loop tl (hd::(fst acc), snd acc)
          else
            loop tl (fst acc, hd::(snd acc))
  in
    loop (List.rev a) ([],[])

(* partition an array into two parts according to a given condition [cond]*)
(*let partition_array cond a =
  let i = ref 0
  and j = ref ((Array.length a) -1) in
    while true do
      if cond a.(j) then decr j;
      if not *)

(* take the first n elements of a list *)
let first_n n a =
  let rec loop n a acc =
    match a with 
        [] -> acc
      | hd::tl ->
          if n > 0 then 
            loop (n-1) tl (hd::acc)
          else
            acc
  in
    List.rev (loop n a [])


(* split a list into the first n elements of a list and the rest *)
let split_first_n n a =
  let rec loop n a (firstn,rest) =
    match a with 
        [] -> (firstn,rest)
      | hd::tl ->
          if n > 0 then 
            loop (n-1) tl (hd::firstn, tl)
          else
            (firstn, hd::tl)
  in
  let x,y = loop n a ([],[]) in (List.rev x, y)

(* tail recursive append function *)
let append a b =
  let rec loop a acc =
    match a with
      [] -> acc
    | hd::tl -> loop tl ( hd :: acc)
  in
    loop (List.rev a) b


(* tail recursive concat function *)
let concat lol =
  let rec loop a acc =
    match a with
      [] -> acc
    | hd::tl -> loop tl ( append hd acc)
  in
    loop (List.rev lol) []

let create_list f n =
  let rec loop i acc = 
    match i with
        0 -> acc
      | i -> loop (i-1) ((f i) :: acc) in
    List.rev (loop (n-1) [])

let timeproc proc =
  let t_start = Unix.gettimeofday () in
  let result = proc () in
  let t_end = Unix.gettimeofday () in
    (result, t_end-.t_start)

let time_and_print proc =
  let (r, t) = timeproc proc in
    printf "\nTime elapsed: %f\n" t;
    r

let time () = Unix.gettimeofday ()

class timer =
object
  val t_s = Unix.gettimeofday ()
  method elapsed = (Unix.gettimeofday ()) -. t_s 
end

let min a b = if a <= b then a else b

let array_iter2 f a1 a2 =
  assert (Array.length a1 = Array.length a2);
  for i=0 to (Array.length a1)-1 do
    f a1.(i) a2.(i);
  done

let array_iter_rev f a =
  for i=(Array.length a)-1 downto 0 do
    f a.(i);
  done

let list_iteri f a =
  let rec loop i a =
    match a with
	[] -> ()
      | hd::tl -> (f i hd; loop (i+1) tl)
  in
    loop 0 a

let list_iter_train f a =
  let a1 = List.tl a 
  and a2 = List.rev (List.tl (List.rev a)) in
    List.iter2 f a2 a1

let list_iter_2comb f a =
  List.iter 
    (fun x-> List.iter (fun y->if x!=y then f x y) (List.tl a) ) a

let max_list a defval = List.fold_left max defval a

let min_list a defval = List.fold_left min defval a

let max_array a defval = Array.fold_left max defval a

let min_array a defval = Array.fold_left min defval a

let argmin_array a  = 
  let minval = ref a.(0) 
  and minidx = ref 0 in
    if debug then assert (Array.length a > 0);
    for i=1 to (Array.length a-1) do
      if a.(i) < !minval  then
	(minval := a.(i);
	 minidx := i)
    done;
    !minidx

let print_list printelt l =
  printf "[ ";
  List.iter (fun x-> printelt x; printf "; ") l;
  printf " ]"

let print_int x = printf "%d" x

let print_float x = printf "%f" x

let print_intlist l = print_list print_int l

let print_floatlist l = print_list print_float l

let print_intpair (x,y) = printf "(%d,%d)" x y

let print_array printelt l =
  printf "[| ";
  Array.iter (fun x-> printelt x; printf "; ") l;
  printf " |]"
 
let print_intarray l = print_array print_int l

let print_floatarray l = print_array print_float l

(*let print_set printelt l =
  printf "{ ";
  .iter (fun x-> printelt x; printf "; ") l;
  printf " }"*)

let prefix_sum a b =
  if debug then assert ((Array.length b) >= (Array.length a));
  let ps = ref 0 in 
    for i=0 to (Array.length a) do
      ps := !ps + a.(i);
      b.(i) <- !ps
    done

let sum_float list = List.fold_left (+.) 0.0 list

let sum list = List.fold_left (+) 0 list

let sum_array a = Array.fold_left (+) 0 a

let array_iter_range f a (s,e) = for i=s to e do f i a.(i) done

(* choose k elements at random from an array 
let choose_k a =
*)


 

let get_option x = 
  match x with
      None -> failwith "trying to get None option"
    | Some y -> y

(* set and association list algorithms *)

(* merge two sets represented as sorted lists a and b
   precond: a and b are sorted in increasing order  *)
let merge_sets (a: (int list)) b =
  let rec loop a b (acc: int list) =
    match a with 
        [] -> append (List.rev b) acc 
      | ahd::atl ->
          (*lprintf "ahd=%d\n" ahd;*)
          match b with 
              [] -> append (List.rev a) acc 
            | bhd::btl ->
                (*lprintf "bhd=%d \n" bhd ;*)
                if ahd < bhd then 
                  loop atl b (ahd::acc)
                else 
                  if ahd = bhd then  
                    loop atl btl (ahd ::acc)
                  else
                    loop a btl (bhd::acc)
  in
    List.rev (loop a b [])

