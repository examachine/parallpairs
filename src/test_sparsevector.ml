open Printf

module SV = SparseVector.SparseVectorListImpl (Types.FloatWeight)


module SV2 = SparseVector.SparseVectorDynArrayImpl (Types.FloatWeight)

let main () =
  printf "testing sparsevector\n";
  let sv = SV.make () in
    SV.append sv (3,0.8);
    SV.append sv (5,0.2);
    SV.print sv;
  let sv = SV2.make () in
    SV2.append sv (3,0.8);
    SV2.append sv (5,0.2);
    SV2.print sv;
    
;;

main ()

  
