(*
  Common parallel algorithms
  Author: Eray Ozkural
*)

open Printf
open Util 

module Weight = Types.Fix32Weight 
open Weight 

let p = Mpi.comm_size Mpi.comm_world
let pid = Mpi.comm_rank Mpi.comm_world 

let debug = false
let profile = true
let log = false

(* print to log *)

let logfile =
  if log then
    open_out ("log." ^ (string_of_int pid))
  else
    open_out "/dev/null"

let lprint_mtx = Mutex.create ()

let lprintf fmt =
  Mutex.lock lprint_mtx;
  let x = fprintf logfile fmt in
    flush logfile; 
  Mutex.unlock lprint_mtx;
  x

let lprint_list printelt l =
  lprintf "[ ";
  List.iter (fun x-> printelt x; lprintf "; ") l;
  lprintf " ]"

let lprint_int x = lprintf "%d" x

let lprint_float x = lprintf "%g" x

let lprint_intlist l = lprint_list lprint_int l

let lprint_intpair (x,y) = lprintf "(%d,%d)" x y

let lprint_array printelt l =
  lprintf "[[ ";
  Array.iter (fun x-> printelt x; lprintf "; ") l;
  lprintf " ]]"

let lprint_intarray l = lprint_array lprint_int l

let lprint_floatarray l = lprint_array lprint_float l

let lprint_wgtarray l = lprint_array (fun x->lprint_float 
                                        (Weight.to_float x)) l

let lprint_gc_stats () = Gc.print_stat logfile

let time_and_print_par proc =
  let (r, t) = timeproc proc in
    lprintf "Time elapsed (local): %g\n" t;
    let tg = Mpi.allreduce_float t Mpi.Float_max Mpi.comm_world in
      lprintf "Time elapsed: %g\n" tg;
      if pid=0 then printf "\nTime elapsed: %g\n" tg;
      r

(* exchange messages between two processors
   using blocking send/recv *)
let exchange_blocking ?(comm=Mpi.comm_world) a src dest =
  let t0 = new timer in
  let src = min src dest
  and dest = max src dest
  and result = ref a in
    if debug then lprintf "src=%d, dest=%d\n" src dest;
    if pid = src then
      (Mpi.send a dest dest comm;
       result := Mpi.receive dest src comm)
    else 
      if pid = dest then
        (result := Mpi.receive src dest comm;
         Mpi.send a src src comm);
    if profile then lprintf "exchange time=%g\n" t0#elapsed;
    !result


(* exchange messages between two processors
   using non-blocking send/recv
BUG: DOESNT WORK WITH THREADS! *)
let exchange ?(comm=Mpi.comm_world) a src dest =
  let t0 = new timer in
  let src = min src dest
  and dest = max src dest
  and result = ref a in
    if debug then lprintf "src=%d, dest=%d\n" src dest;
    if pid = src then
      (let req = Mpi.isend a dest dest comm in
         result := Mpi.receive dest src comm;
         Mpi.wait req)
    else 
      if pid = dest then
        (let req = Mpi.isend a src src comm in
           result := Mpi.receive src dest comm;
           Mpi.wait req);
    if profile then lprintf "exchange time=%g\n" t0#elapsed;
    !result
        
(* exchange messages between two processors
   using blocking send/recv *)
let exchange_int_array a src dest =
  let src = min src dest
  and dest = max src dest
  and b = Array.make (Array.length a) 0 in
    if debug then lprintf "src=%d, dest=%d\n" src dest;
    if pid = src then
      (Mpi.send_int_array a dest dest Mpi.comm_world;
       Mpi.receive_int_array b dest src Mpi.comm_world)
    else 
      if pid = dest then
        (Mpi.receive_int_array b src dest Mpi.comm_world;
         Mpi.send_int_array a src src Mpi.comm_world );
    b

(*
  concurrent clique AAPC 
  a.(dest) = message to send to processor with pid dest
  returns r where r.(src) = messages received from processor r
*)

let aapc ?(comm=Mpi.comm_world) (a: 'a array)  : ('a array) = 
  let p = Mpi.comm_size comm and pid = Mpi.comm_rank comm in
  let recv_req = Array.make p Mpi.null_request in
  let send_req = Array.make p (Mpi.null_request, Mpi.null_request) in
    (*lprintf "clique AAPC\n";*)
    (* exchange messages *)
    for partner = 0 to p-1 do
      if partner != pid then (
        (*lprintf "sending to %d\n" partner;*)
        send_req.(partner) <- Mpi.isend_varlength a.(partner) partner pid comm         )
    done;
    for partner = 0 to p-1 do 
      if partner != pid then
        ((*lprintf "receiving from %d\n" partner;*)
         recv_req.(partner) <- Mpi.ireceive_varlength partner partner comm;
        )
    done;
    (*lprintf "ireceives posted\n";*)
    Array.init p 
      (fun i->if i=pid then a.(i) else Mpi.wait_receive recv_req.(i)) 


(*
  blocking clique AAPC 
  a.(dest) = message to send to processor with pid dest
  returns r where r.(src) = messages received from processor r

  buggy, doesn't work hangs 
*)

(*
let aapc_blocking ?(comm=Mpi.comm_world) (a: 'a array)  : ('a array) = 
  let p = Mpi.comm_size comm and pid = Mpi.comm_rank comm in
    if debug then lprintf "AAPC with blocking comms\n";
    (* exchange messages *)
    for partner = 0 to p-1 do
      if partner != pid then (
        if debug then lprintf "sending to %d\n" partner;
        Mpi.send a.(partner) partner pid comm         )
    done;
    Array.init p 
      (fun i-> if i=pid then a.(i) else Mpi.receive i i comm ) 
*)

(* multi node accumulation to all processors on hypercube network, 
   the equivalent of MPI Allreduce operation, 
  a is an input object of any type
  op a b = accumulates two objects a and b
*)
let hypercube_mnac_all ?(comm=Mpi.comm_world) (a: 'a) op : 'a  =
  let p = Mpi.comm_size comm and pid = Mpi.comm_rank comm in
  let d = Util.log2_int p in
  let result = ref a in
    if debug then lprintf "d=%d\n" d;
    for dim=d-1 downto 0 do (* process dimensions of hypercube *)
      let partner = pid lxor (1 lsl dim) in
        if debug then lprintf "partner=%d\n" partner;
        let partners_result = exchange ~comm:comm !result pid partner in 
          result := op !result partners_result
    done;
    !result

(*
  multi node accumulation with output partitioning on hypercube 
  a is an input list (sequence really)
  op a b partition = combines two objects a and b for dest processor
  where partition i x determines the home processor of element x of 
  a with index i.  
*)

(*let mnac (a: 'a) op : 'a  =
  let d = Util.log2_int p in
  let result = ref a in
    if debug then lprintf "d=%d\n" d;
    for dim=d-1 downto 0 do (* process dimensions of hypercube *)
      let partner = pid lxor (1 lsl dim) in
        if debug then lprintf "partner=%d\n" partner;
        let partners_result = exchange !result pid partner in 
          result := op !result partners_result
    done;
    !result
*)

(*let par_fold_list op defval a = 
  Array.fold_left (List.fold_left max defval a)
*)


(* parallel quicksort on hypercube
   use the hypercube property that H_d is composed of two H_{d-1} cubes
   the partitioning of list elements is parallelized by exchanging 
   elements in parallel.
   a = integer list TODO any list
   TODO comp x y = compares two objects in list a
*)

let hypercube_quicksort a  =
  let comm = ref Mpi.comm_world in
  let par_partition a cube =
    if debug then lprintf "par_partition cube=%d\n" cube;
    let nprocs = Mpi.comm_size !comm
    and id = Mpi.comm_rank !comm in
    let color = if pid land (1 lsl cube)=0 then 0 else 1 in
      if debug then 
        lprintf "in subcube: nprocs=%d, id=%d, color=%d\n" nprocs id color;
      let xnz,xlocal = 
        if a!=[] then
          1, choose_random (Array.of_list a)
        else
          0, 0 in
      let myarray = Array.of_list [xnz; xlocal] in
      let sumarray = Array.make 2 0 in
        Mpi.allreduce_int_array myarray sumarray Mpi.Int_sum !comm;
        let nzprocs,xsum = sumarray.(0), sumarray.(1) in
        let x = if nzprocs=0 then 0 else xsum/nzprocs in   
          if debug then lprintf "pivot=%d\n" x;
          comm := Mpi.comm_split !comm color pid;
          partition (fun elt->elt<=x) a
 in
  let d = Util.log2_int p in
  let b = ref a in (* accumulation buffer *)
    if debug then lprintf "d=%d\n" d;
    for i=d-1 downto 0 do (* process dimensions of hypercube *)
      let link = 1 lsl i in
      let partner = pid lxor link in
        if debug then lprintf "i=%d, partner=%d\n" i partner;
        let b0,b1 = par_partition !b i in
        if debug then lprintf "b partitioned\n"; 
        let tokeep = (if pid land link=0 then b0 else b1) 
        and tosend = (if pid land link=0 then b1 else b0) in
        let received = exchange tosend pid partner in 
          if debug then 
            (lprintf "tokeep="; lprint_intlist tokeep;
             lprintf "tosend="; lprint_intlist tosend;
             lprintf "received="; lprint_intlist received);
          b := Util.append tokeep received;
          if debug then (lprintf "b="; lprint_intlist !b; lprintf "\n")
    done;
    List.sort compare !b
  

let gather_time = ref 0.0
let barrier_time = ref 0.0

let measure_barrier comm = 
  if profile then (
    let t0 = new timer in
      Mpi.barrier comm; 
      barrier_time := !barrier_time +. t0#elapsed;
      (*lprintf "barrier time %g\n" t0#elapsed;*)
  )

let reduce_all ?(comm=Mpi.comm_world) x op def = 
  measure_barrier comm;
  let t1 = new timer in
  let xa = Mpi.allgather x comm in
    gather_time := !gather_time +. t1#elapsed;
    Array.fold_left op def xa

let check_memory () =
  let f = open_in "/proc/self/status" in
  let vlimit = 128 * 1024 * 1024 / (max 48 p) in
    (try 
      let found = ref false in
        while not !found do
          let l = input_line f in
            if (String.sub l 0 6) = "VmSize" then
              (found := true;
               let elts = Str.split (Str.regexp "[ \t]+") l in
               let vmem = int_of_string (List.nth elts 1) in
                 if vmem>vlimit then
                   failwith "Virtual memory limit exceeded: %dkb" )
        done;
     with End_of_file -> ());
    close_in f
