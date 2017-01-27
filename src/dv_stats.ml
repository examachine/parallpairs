open Printf
open Scanf
open Arg
open AllPairs
open Util


module DV = Dv.Make (Types.FloatWeight)
open DV

let in_fname = ref ""
  
let usage =  "Usage: dv-stats <in-file> <out-file> <threshold>"



(* maximum dimension in a document vector *)
let max_dim_dv dv = 
  Util.max_list (Array.to_list (Array.map (fun x->x.term) dv.vector)) 0

let max_dim v = 
  Util.max_list (Util.map max_dim_dv v) 0

let calc_dim_nz v m = 
  let dim_nz = Array.make m 0 in
    List.iter
      (fun x ->
         Array.iter
           (fun dvelt->
              dim_nz.(dvelt.term) <- dim_nz.(dvelt.term) + 1 
           ) x.vector
      ) v;
    dim_nz
 

let main () =
  if ((Array.length Sys.argv)!=2) then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  in_fname := Sys.argv.(1); 
  printf "Reading DV file %s\n" !in_fname; flush stdout;
  let v = DV.read_dv !in_fname in
  (*let prefix = String.sub !in_fname 0 ((String.length !in_fname)-3) in*)
    printf "Read %d vectors\n" (List.length v); flush stdout;
    let m = max_dim v in
    let dim_nz = calc_dim_nz v m in 
    let num_dims = sum_array (Array.map (fun x->if x>0 then 1 else 0) dim_nz) in
    let n = List.length v in
    let n_f = float_of_int n and m_f = float_of_int num_dims in
    let num_nz = sum_array dim_nz in 
    let sparsity = (float_of_int num_nz) /. (n_f *. m_f) in
      printf "number of vectors=%d\n" n;
      (*printf "number of dimensions=%d\n" m;*)
      printf "number of non-empty dimensions=%d\n" num_dims;
      printf "number of nonzeroes=%d\n" num_nz;
      printf "avg vector size = %g\n" ( (float_of_int num_nz) /. n_f);
      printf "avg dimension size = %g\n" ( (float_of_int num_nz) /. m_f);
      printf "sparsity=%g\n" sparsity;
      (* TODO: write histogram to file *)
      let dims = Util.combine (Util.range 0 (m-1)) (Array.to_list dim_nz)
      and order = Array.make m 0 in
      let hist_file = open_out (!in_fname ^ ".hist") in
      let sorted_dims = 
        List.sort (fun a b -> compare (snd b) (snd a)) dims in
        printf "histogram = ";
        List.iter2 (fun i x ->
                      order.(fst x) <- i;
                      if (snd x) > 0 then
                        fprintf hist_file "(%d,%d) " (fst x) (snd x) 
                   ) (Util.range 0 (m-1)) sorted_dims;;

main();

