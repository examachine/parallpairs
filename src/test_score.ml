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

open Parallel
open Util
open Score
open ScoreAccum

module Weight = Types.Fix32Weight
open Weight

let main () =
  printf "testing merge_assoc_lists\n";
  let a = [ (5,Weight.of_float 0.3); (2,Weight.of_float 0.6); (10,Weight.of_float 0.1)] 
  and b = [(10,Weight.of_float 0.2); (5, Weight.of_float 0.2); (4, Weight.of_float 0.4);] 
  and c = [(1, Weight.of_float 0.1); (0,Weight.of_float 0.9)] in
    (let m = Score.merge_assoc_lists [ []; [] ] in
       printf "test merging of two empty assoc lists";
       print_list (fun (x,y) -> printf "(%d,%f)" x (Weight.to_float y)) m;
       printf "\n");
    (let m = Score.merge_assoc_lists [a;b] in
       print_list (fun (x,y) -> printf "(%d,%f)" x (Weight.to_float y)) m;
       printf "\n");
    (printf "test new al merge routine, merge a b\n";
     let m = Score.merge_als (Score.sort_al a) (Score.sort_al b) in
       print_list (fun (x,y) -> printf "(%d,%f)" x (Weight.to_float y)) m;
       printf "\n");
    (printf "test new al merge routine, merge b c\n";
     let m = merge_als (sort_al b) (sort_al c) in
       print_list (fun (x,y) -> printf "(%d,%f)" x (Weight.to_float y)) m;
       printf "\n");
    ( let a = [3;5;6;8] and b = [2;4;5;6;7;9] and c = [3;5] in
        printf "test merge sets routine, merge a b=";
        print_intlist (merge_sets a b);
        printf "test merge sets routine, merge b c=";
        print_intlist (merge_sets b c);
        printf "test merge sets routine, merge a c=";
        print_intlist (merge_sets a c);
        printf "\n"
    );
    (
      printf "test merge dynarray sets\n";
      let a = Dynarray.make 0 in
      let b = Dynarray.make 0 in
        printf "a = "; Dynarray.print_int a;
        printf ", b = "; Dynarray.print_int b;
        let r = merge_dynarray_sets a b in
          printf ", result = "; Dynarray.print_int r;
          print_newline ()
    );
    (
      printf "test merge dynarray sets\n";
      let a = Dynarray.make 0 in
      let b = Dynarray.make 0 in
        Dynarray.append a 1;
        Dynarray.append a 2; 
        Dynarray.append b 1;
        Dynarray.append b 2;
        Dynarray.append b 5;
        printf "a = "; Dynarray.print_int a;
        printf ", b = "; Dynarray.print_int b;
        let r = merge_dynarray_sets a b in
          printf ", result = "; Dynarray.print_int r;
          print_newline ()
    );
    (
      printf "test merge dynarray sets\n";
      let a = Dynarray.make 0 in
      let b = Dynarray.make 0 in
        Dynarray.append a 0;
        Dynarray.append a 1;
        Dynarray.append a 3;
        Dynarray.append a 5;
        Dynarray.append a 9; 
        Dynarray.append b 1;
        Dynarray.append b 5;
        Dynarray.append b 6;
        printf "a = "; Dynarray.print_int a;
        printf ", b = "; Dynarray.print_int b;
        let r = merge_dynarray_sets a b in
          printf ", result = "; Dynarray.print_int r;
          print_newline ()
    );
    (
      printf "test merge dynarray pair merge\n";
      let ay,aw = DynarrayPair.make 0 0 in
      let by,bw = DynarrayPair.make 0 0 in
        printf "a = "; Dynarray.print_int ay; Dynarray.print_int aw;
        printf "b = "; Dynarray.print_int by; Dynarray.print_int bw;
        let (ay',aw') = merge_dynarray_pair_sets (ay,aw) (by,bw) in
          printf " result = ";
          Dynarray.print_int ay'; Dynarray.print_int aw';
          print_newline ()
    );
    (
      printf "test merge dynarray pair merge\n";
      let ay,aw = DynarrayPair.make 0 0 in
      let by,bw = DynarrayPair.make 0 0 in
        DynarrayPair.append (ay,aw) 1 5;
        DynarrayPair.append (ay,aw) 2 3; 
        DynarrayPair.append (by,bw) 1 3;
        DynarrayPair.append (by,bw) 2 1;
        DynarrayPair.append (by,bw) 5 2;
        printf "a = "; Dynarray.print_int ay; Dynarray.print_int aw;
        printf "b = "; Dynarray.print_int by; Dynarray.print_int bw;
        let (ay',aw') = merge_dynarray_pair_sets (ay,aw) (by,bw) in
          printf " result = ";
          Dynarray.print_int ay'; Dynarray.print_int aw';
          print_newline ()
    );
    (
      printf "test merge dynarray pair merge\n";
      let ay,aw = DynarrayPair.make 0 0 in
      let by,bw = DynarrayPair.make 0 0 in
        DynarrayPair.append (ay,aw) 0 3;
        DynarrayPair.append (ay,aw) 4 8;
        DynarrayPair.append (ay,aw) 6 2;
        DynarrayPair.append (by,bw) 1 2;
        DynarrayPair.append (by,bw) 3 2;
        DynarrayPair.append (by,bw) 4 5;
        printf "a = "; Dynarray.print_int ay; Dynarray.print_int aw;
        printf "b = "; Dynarray.print_int by; Dynarray.print_int bw;
        let (ay',aw') = merge_dynarray_pair_sets (ay,aw) (by,bw) in
          printf " result = ";
          Dynarray.print_int ay'; Dynarray.print_int aw';
          print_newline ()
    )
;;

main ()
