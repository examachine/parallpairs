open Printf

module Fix32 = Types.Fix32Weight

let main () =
  let a = Fix32.of_float 0.2 
  and b = Fix32.of_float 0.3 in
  let c = Fix32.mul a b in
    printf "a=%f, b=%f, a*b=%f\n"
      (Fix32.to_float a) (Fix32.to_float b) (Fix32.to_float c);;

main ()

