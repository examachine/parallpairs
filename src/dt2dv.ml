open Printf
open Scanf
open Arg
open AllPairs
open Util


module DV = Dv.Make (Types.FloatWeight)

let dt_fname = ref ""
let dv_fname = ref ""

let speclist = 
  [
    ("-dt", String (fun x -> dt_fname := x), "document-term matrix file (svm)");
    ("-dv", String (fun x -> dv_fname := x), "document vector file")
  ]

let usage =  "Usage: dt2dv -dt <dt-file> -dv <dv-file>"

let main () =
  Arg.parse speclist (fun x -> ()) usage;
  if (!dt_fname="" or !dv_fname="") then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );
  printf "Reading DT file %s\n" !dt_fname; flush stdout;
  let dv_list = DV.read_dv_svm !dt_fname in
    printf "Read %d vectors\n" (List.length dv_list); flush stdout;
    printf "Writing DV file %s\n" !dv_fname; flush stdout;
    DV.write_dv !dv_fname dv_list;;

main();


    
