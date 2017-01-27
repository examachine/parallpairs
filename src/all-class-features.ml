open Printf
open Scanf
open Arg
open DistanceMatrix
open ClassFeatures
open Category

let cat_fname = ref ""
let data_dir = ref ""
let feature_sfx = ref ""

let speclist = 
    [
      ("-cat", String (fun x -> cat_fname := x), "category file");
      ("-dir", String (fun x -> data_dir := x), "data dir");
      ("-feature", String (fun x -> feature_sfx := x), "feature")
    ]

let usage =
  "Usage: all-class-features -cat <category-file> -dir <data-dir> \
-feature <feature-set> "

let process_category cat =
  printf "%s... %!" cat;
  let dm_name = Filename.concat !data_dir (cat ^ ".dm") in
  let class_dir = 
    Filename.concat (Filename.concat !data_dir "docs") cat in
  let d = read_dist_matrix dm_name in
  let feature_set = ClassFeatures.feature_set !feature_sfx in
  let (f, features) = feature_set num_features d class_dir in
    printf "# features: %d\n" (Array.length features);
    write_features (dm_name ^ "." ^ !feature_sfx ^ ".features") features;
    write_mtx (dm_name ^ "." ^ !feature_sfx) f

let main () =
  printf "Compute features for all classes\n%!";
  Arg.parse speclist (fun x -> ()) usage;
  if (!cat_fname="" or !data_dir="" or !feature_sfx="") then
    (printf "%s\n" usage; exit 2);
  Printf.printf "Reading categories from %s\n %!" !cat_fname;
  let cats = read_cats !cat_fname in
    Printf.printf "Processing %d categories\n%!" (Array.length cats);
    Array.iter process_category cats;
    print_newline ();
    ;;
    

main ();
