open Printf
open Scanf
open Arg
open Bigarray
open Util
open DistanceMatrix
open ClassFeatures
open SvmFeatures

let read_cats fname = 
  let buf = Scanning.from_file fname in
  let cat_list = ref [] in
    while not (Scanning.end_of_input buf) do
      begin
	bscanf buf "%s "
	  (fun cat -> cat_list := cat :: !cat_list)
      end
    done;
    Array.of_list (List.rev !cat_list)


let cat_fname = ref ""
let data_dir = ref ""
let feature_set = ref ""

let speclist = 
  [
    ("-cat", String (fun x -> cat_fname := x), "category file");
    ("-dir", String (fun x -> data_dir := x), "data dir");
    ("-feature", String (fun x -> feature_set := x), "feature")
  ]

let usage =  "Usage: merge-features -cat <category-file> -dir \
<data-dir> -feature <feature-set>"


let data_file file = Filename.concat !data_dir file

let class_dir cat = 
  Filename.concat (Filename.concat !data_dir "docs") cat

let doc_file cat file =
  Filename.concat (class_dir cat) file

let cat_feature cat feature = 
  Filename.concat !data_dir (cat ^ ".dm." ^ feature ^ ".features")

let doc i cat file = {file=doc_file cat file; cat = i}

(* return an array of files in a class *)
let cat_files i cat =
  Array.of_list
    (List.map (doc i cat) (read_dir_files (class_dir cat)))

(* return an array of features of a class *)
let cat_features feature_set cat =
  let fname = cat_feature cat feature_set in
    printf "reading features for cat %s from: %s\n%!" cat fname;
    let features =
      List.map (fun f -> {ix=f.ix; w=f.w;
			  name=Filename.concat (class_dir cat) f.name})
	(read_features fname) in
	Array.of_list features

let main () =
  printf "Merge a requested feature set of all classes for SVMLIB data\n%!";
  Arg.parse speclist (fun x -> ()) usage;
  if (!cat_fname="" or !data_dir="" or !feature_set="") then (
    printf "Missing arguments\n%s\n" usage;
    exit 2;
  )
  Printf.printf "Reading categories from %s: %!" !cat_fname;
  let cats = read_cats !cat_fname in
  let catsdm =
    Array.map (fun x -> Filename.concat !data_dir (x ^ ".dm")) cats in
    Array.iter (Printf.printf "%s ") cats;
    print_newline ();
    let filesmtx = Array.mapi cat_files cats
    and featuresvec =
      Array.map (cat_features !feature_set) cats in
    let catlen = Array.map Array.length filesmtx in
    let docs = merge_files filesmtx in
    let features = merge_features featuresvec catlen in
      printf "Total # docs: %d\n%!" (Array.length docs);
      printf "Total # features: %d\n%!" (Array.length features);
      write_features
	(data_file "radikal.svm." ^ !feature_set ^ ".features") features;
      write_docs (data_file "radikal.svm." ^ !feature_set ^ ".docs") docs;
      printf "END RUN\n"
;;

main ();
