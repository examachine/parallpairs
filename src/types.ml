

module type WeightType = 
sig
  type t
  val zero: t
  val compare : t -> t -> int
  val add: t -> t -> t
  val sub: t -> t -> t
  val mul: t -> t -> t
  val div: t -> t -> t
  val (+:): t -> t -> t
  val (-:): t -> t -> t
  val ( *: ): t -> t -> t
  val (/:): t -> t -> t
  val sqrt: t -> t
  val of_float: float -> t
  val to_float: t -> float
    (*val print: out -> t -> unit*)
end

(*TODO: get rid of compare? *)
module WeightCommon = 
struct
  let (>=:) x y = compare x y >= 0 
  let (<:) x y = compare x y < 0 
  let max x y = if compare x y >= 0 then x else y
  let min x y = if compare x y <= 0 then x else y
end

module type FloatWeight = WeightType

module FloatWeight =
struct
  type t = float
  let zero = 0.0
  let compare x y = if x<=y then -1 else (if x=y then 0 else 1)
  let add x y = x +. y
  let sub x y = x -. y
  let mul x y = x *. y
  let div x y = x /. y
  let (+:) x y = x +. y
  let (-:) x y = x -. y
  let ( *: ) x y = x *. y
  let (/:) x y = x /. y
  let sqrt x = sqrt x
  let of_float x = x
  let to_float x = x

  include WeightCommon
end
(*
module type Fix32Weightold = WeightType

module Fix32Weightold =
struct
  type t = Int32.t
  let zero = Int32.zero

  let q = 22 (* number of fixed point bits *)
  let k = Int32.of_int (1 lsl q) (* implementation dependent, max 28 *)
  let kf = float_of_int (1 lsl q)

  let compare x y = if x<=y then -1 else (if x=y then 0 else 1)
  let add x y = Int32.add x y
  let sub x y = Int32.sub x y
  let mul x y =
    Int64.to_int32 
      (Int64.shift_right (Int64.mul (Int64.of_int32 x) (Int64.of_int32 y) ) q)
  let div x y = 
    Int64.to_int32 (Int64.div (Int64.shift_left (Int64.of_int32 x) q) 
                      (Int64.of_int32 y))
  let (+:) x y = add x  y
  let (-:) x y = sub x  y
  let ( *: ) x y = mul x  y
  let (/:) x y = div x y

  let of_float x = Int32.of_float (kf *. x) 
  let to_float x = (Int32.to_float x) /. kf

  let sqrt x = (of_float (sqrt (to_float x)))

  include WeightCommon
end

module type Fix32Weight64bit = WeightType

module Fix32Weight64bit =
struct
  type t = int
  let zero = 0

  let q = 22 (* number of fixed point bits *)
  let k = 1 lsl q (* implementation dependent, max 28 *)
  let kf = float_of_int (1 lsl q)

  let compare x y = x - y
  let add x y = x + y
  let sub x y = x - y
  let mul x y =
    Nativeint.to_int 
      (Nativeint.shift_right 
         (Nativeint.mul (Nativeint.of_int x) (Nativeint.of_int y) ) q)
  let div x y = 
    Nativeint.to_int
      (Nativeint.div (Nativeint.shift_left (Nativeint.of_int x) q) 
         (Nativeint.of_int y))
  let (+:) x y = add x  y
  let (-:) x y = sub x  y
  let ( *: ) x y = mul x  y
  let (/:) x y = div x y

  let of_float x = int_of_float (kf *. x) 
  let to_float x = (float_of_int  x) /. kf

  let sqrt x = (of_float (sqrt (to_float x)))

  include WeightCommon
end
*)

module type Fix32Weight = WeightType

module Fix32Weight =
  struct

  type t = int
  let zero = 0

  let q = 22 (* number of fixed point bits *)
  let k = 1 lsl q (* implementation dependent, max 28 *)
  let kf = float_of_int (1 lsl q)
  let qx = q/2
  let qy = q-qx
  let n = 30

  let compare x y = x - y
  let add x y = x + y
  let sub x y = x - y
  let mul x y =
    Int64.to_int 
      (Int64.shift_right 
         (Int64.mul (Int64.of_int x) (Int64.of_int y) ) q)
  let mulfast x y = 
    (*if debug then assert (x<k && y<k)*)
    ((x asr (qx-4)) * (y asr (qy-4))) asr 8

  let div x y = 
    Int64.to_int
      (Int64.div (Int64.shift_left (Int64.of_int x) q) 
         (Int64.of_int y))      
  let (+:) x y = add x  y
  let (-:) x y = sub x  y
  let ( *: ) x y = mulfast x  y
  let (/:) x y = div x y

  let of_float x = int_of_float (kf *. x) 
  let to_float x = (float_of_int  x) /. kf

  let sqrt x = (of_float (sqrt (to_float x)))

  include WeightCommon
  end

(*
module type Fix16Weight = WeightType

(* TODO: rounding? *)
module Fix16Weight =
struct
  type t = Int32.t
  let zero = Int32.zero

  let q = 16 (* number of fixed point bits *)
  let k = Int32.of_int (1 lsl q) (* implementation dependent, max 28 *)
  let kf = float_of_int (1 lsl q)

  let compare x y = if x<=y then -1 else (if x=y then 0 else 1)
  let add x y = Int32.add x y
  let sub x y = Int32.sub x y
  let mul x y = Int32.shift_right (Int32.mul x y) q
  let div x y = Int32.div (Int32.shift_left x q) y 
  let (+:) x y = add x y
  let (-:) x y = sub x y
  let ( *: ) x y = mul x y
  let (/:) x y = div x y

  let of_float x = Int32.of_float (kf *. x) 
  let to_float x = (Int32.to_float x) /. kf

  let sqrt x = (of_float (sqrt (to_float x)))

  include WeightCommon
  (*
  let (>=:) x y = x >= y 
  let (<:) x y = x < y 
  let max x y = if x >= y then x else y
  let min x y = if x <= y then x else y
  *)
end
*)
