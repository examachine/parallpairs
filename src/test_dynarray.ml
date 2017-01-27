open Printf
 
let main () =
  printf "testing dynarray\n";
  let ay,ad = DynarrayPair.make 0 0 in
    Dynarray.append ay 3; Dynarray.append ad 8;
    Dynarray.append ay 5; Dynarray.append ad 2;
    Dynarray.append ay 1; Dynarray.append ad 4;
    let (ay', ad') = DynarrayPair.sort_first compare (ay,ad) in
      Dynarray.print_int ay'; Dynarray.print_int ad'
;;

main ()

  
