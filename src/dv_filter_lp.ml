open Printf
open Scanf
open Arg
open AllPairs
open Util


module DV = Dv.Make (Types.FloatWeight)
open DV

let in_fname = ref ""
let out_fname = ref ""
let threshold = ref 0

(*let speclist = 
  [
    ("-dt", String (fun x -> dt_fname := x), "document-term matrix file (svm)");
    ("-dv", String (fun x -> dv_fname := x), "document vector file")
  ]
*)
let usage =  "Usage: filter-dv-lp <in-file> <out-file> <threshold>"



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

let split_dv dim_nz sigma dv = 
  let len = Array.length dv.vector 
  and flag = ref false and num_h = ref 0 in
    while not !flag do
      if !num_h < Array.length dv.vector then (
        let termfreq = dim_nz.(dv.vector.(!num_h).term) in
          if termfreq < sigma then (
            flag := true;
            decr num_h;
          );
          incr num_h 
      )
      else
        flag := true
    done;
    let dvh = DV.make_dv dv.docid (Array.sub dv.vector 0 !num_h) dv.cat
    and dvl = DV.make_dv dv.docid (Array.sub dv.vector !num_h 
                                       (len - !num_h)) dv.cat in
      (dvh, dvl)

      

let lowpass v s = 
  let m = 1 + max_dim v in 
  let dim_nz = calc_dim_nz v m in
  let vsplit = Util.map (split_dv dim_nz s) v in
  let vh,vl = Util.split vsplit in
    vl


let main () =
  if ((Array.length Sys.argv)!=4) then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  in_fname := Sys.argv.(1);
  out_fname := Sys.argv.(2);
  threshold := int_of_string Sys.argv.(3);
  printf "Reading DV file %s\n" !in_fname; flush stdout;
  let dv_list = DV.read_dv !in_fname in
    printf "Read %d vectors\n" (List.length dv_list); flush stdout;
    let dv_lp = lowpass dv_list !threshold in
      printf "Writing low-pass filtered DV file %s\n" !out_fname; flush stdout;
      DV.write_dv !out_fname dv_lp;;

main();


    
