

let make x y = (Dynarray.make x, Dynarray.make y)

let make_reserved x na y nb  = (Dynarray.make_reserved x na, 
                                Dynarray.make_reserved y nb)

let init f g  = (Dynarray.init 0 f, Dynarray.init 0 f)

let append (a,b) x y = Dynarray.append a x; Dynarray.append b y

(* parallel iteration where f x y is applied to elements of arrays a and b *)
let iter f (a,b) = 
  assert(Dynarray.length a = Dynarray.length b);
  for i = 0 to (Dynarray.length a)-1 do
    f (Dynarray.get a i) (Dynarray.get b i)
  done

(* sort parallel arrays where f (x1,y1) (x2,y2) compares elements of
   two array pairs. non-destructive.
*)

(* sort with a comparison function given on first array *)
let sort_first cmp (a,b) = 
  let n = Dynarray.length a in
    assert(n = Dynarray.length b);
    let ix = Array.init (Dynarray.length a) (fun i->i) in 
      (* sort indices according to corresponding elements in a *)
      Array.sort (fun x y->cmp (Dynarray.get a x) (Dynarray.get a y)) ix; 
      (* constructed sorted arrays *)
      (Dynarray.init n (fun i->Dynarray.get a ix.(i)),
       Dynarray.init n (fun i->Dynarray.get b ix.(i)))
      
let length (a,b) = Dynarray.length a

