open Printf
open Scanf
open Arg
open AllPairs
open Multilevel
open Util

let dv_fname = ref ""
let threshold = ref (-1.0)
let simplexsize = ref 2
let sigma = ref 2
let th = ref 0.1
let normalize = ref true

type algotype = int option

let algoix = ref None
let whichalgo aix = match aix with
    None -> -1
  | Some x -> x


let list_algos = ref false

let speclist = 
  [
    ("-dv", String (fun x -> dv_fname := x), "document vector");
    ("-threshold", Float (fun x -> threshold := x), "similarity threshold");
    ("-simplexsize", Int (fun x -> simplexsize := x), "size of coarsest dataset");
    ("-sigma", Int (fun x -> sigma := x), "frequent term cutoff threshold");
    ("-th", Float (fun x -> th := x), "epsilon_h");
    ("-algo", String (fun x -> algoix := Some (int_of_string x)), 
     "algorithm to run");
    ("-nonorm", Unit (fun () -> normalize := false), "do not normalize");
    ("-list", Unit (fun () -> list_algos := true), "list algorithms")
  ]

let usage =  "Usage: all-pairs-multilevel -dv <dv-file> -threshold <sim-threshold> -algo <algorithm-number> [-list]"

module FloatDV = Dv.Make (Types.FloatWeight)
module ConvertDV = Dv.Converter (FloatDV) (MyDV)

let main () =
  let plainalgo f = fun v->f v !threshold in
  let algos = 
    [ ( (fun v->all_pairs_multilevel ~simplex_size:(!simplexsize) v !threshold), "all-pairs-multilevel"); 
    ] in
  Arg.parse speclist (fun x -> ()) usage;
  if !list_algos then
    (list_iteri (fun i (_,name) -> printf "%d: %s\n" i name) algos;
     exit 0
    );
  if (!dv_fname="" or !threshold<0.0) then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  );  printf "Reading DV file %s\n" !dv_fname; flush stdout;
  let floatdv_list = 
    if !normalize then (
      printf "Normalizing\n";
      Util.map FloatDV.normalize (FloatDV.read_dv !dv_fname)
    )
    else 
      FloatDV.read_dv !dv_fname
  in
  let fixdv_list = Util.map ConvertDV.convert floatdv_list in
  let run (algo, name) =
    printf "\n* Running %s\n" name; flush stdout;
    let r = time_and_print (fun ()->algo fixdv_list)
    and f = open_out (!dv_fname ^ "." ^ name) in
      printf "Number of matches: %d\n" (AllPairs.Docmatch_Vec.cardinal r);
      AllPairs.Docmatch_Vec.iter (fun (x,y,w) -> fprintf f "%d %d: %f\n" 
                                    x y (Weight.to_float w)) r;
      close_out f in
    if !algoix!=None then
      run (List.nth algos (whichalgo !algoix))
    else
      List.iter 
        (fun (algo, name) -> run (algo, name)) algos;;

main();


    
