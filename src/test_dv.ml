open Printf

module DV = Dv.Make (Types.FloatWeight)


let main () =
  let x = DV.emptydvelt in
    printf "testing dv\n";
    for docid=0 to 10 do
      printf "doc %d\n" docid;
      let dv_list = List.map 
        (fun i-> (Random.int 1000,
                  Types.FloatWeight.of_float (Random.float 1.0)) )
        (Util.range 1 10) in
        DV.print (DV.of_list dv_list 0 0);
        printf "\n";
    done;;
      
main ();;

