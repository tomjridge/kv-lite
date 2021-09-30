(** Implementation using LMDB *)

open Util

module M = struct
  type 'a t = 'a
end

let error_to_exn (x:'a or_error) = 
  match x with
  | Ok x -> x
  | Error e -> failwith e

(* NOTE for clear, we want to just reopen with a clean database *)
type t = {
  fn : string;
  mutable env             : Lmdb.Env.t;
  mutable map             : (string,string,[`Uni]) Lmdb.Map.t;
  mutable error_hook      : string -> unit;
  mutable timer           : (unit -> float); 
  mutable last_batch_time : float;    
}

let timer = Unix.time
let last_batch_time = 0.0

(* NOTE this prints the error and exits; use set_error_hook for more
   nuanced behaviour *)
let default_error_hook = fun s -> 
  Printf.printf "Fatal error: %s\n%!" s;
  Stdlib.exit (-1)

module L = Lmdb

(* FIXME need to allow users to open a file which may be bigger than this *)
let const_1TB = 1099511627776

let open_ ~fn = 
  let env : L.Env.t = L.Env.(create Rw ~map_size:const_1TB ~flags:Flags.no_subdir fn) in 
  let map = L.(Map.(create Nodup) ~key:Conv.string ~value:Conv.string) env in
  Ok { fn; env; map; error_hook=default_error_hook; timer; last_batch_time }

let create ~fn = 
  (* delete and make a new one? *)
  (try Sys.remove fn with _ -> ());
  open_ ~fn
    
let close t = L.Env.close t.env

let set_error_hook t f = t.error_hook <- f

let slow_insert t k v = L.Map.set t.map k v

let slow_delete t k = L.Map.remove t.map k

let find_opt t k = 
  try L.Map.get t.map k |> fun x -> Some x
  with Not_found -> None

let batch t ops =
  let t1 = t.timer () in
  L.(Txn.go Rw t.env) begin fun txn ->  
    ops |> List.iter (function
        | (k,`Insert v) -> L.Map.set t.map ~txn k v
        | (k,`Delete) -> L.Map.remove t.map ~txn k);
    ()
  end |> function
  | None -> 
    failwith "impossible: transaction aborted" 
  (* NOTE impossible for a user abort; FIXME presumably this also
     happens if something goes wrong (eg NOSPC), so this can actually
     happen and we should deal with it *)
  | Some () -> 
    let t2 = t.timer () in
    t.last_batch_time <- t2 -. t1;
    ()

let clear t =
  close t;
  create ~fn:t.fn |> function
  | Ok x -> 
    t.env <- x.env;
    t.map <- x.map;
    ()
  | Error _e -> 
    (* NOTE impossible, from defn of create *)
    failwith "impossible"

let last_batch_duration t = t.last_batch_time

let set_time t f = t.timer <- f      

let sync t = L.Env.sync ~force:true t.env

    
