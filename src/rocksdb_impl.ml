(** Implementation using RocksDB *)

open Util

module M = struct
  type 'a t = 'a
end
open M

(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

type 'a or_error = ('a,string)Stdlib.result

let ( >>= ) m f = m |> f
let return (x:'a) : 'a t = x

let error_to_exn (x:'a or_error) = 
  match x with
  | Ok x -> return x
  | Error e -> failwith e

(* NOTE for clear, we want to just reopen with a clean database *)
type t = {
  mutable db: Rocksdb.t;
  fn : string;
  mutable error_hook      : string -> unit;
  mutable timer           : (unit -> float); 
  mutable last_batch_time : float;
    
}

let timer = Sys.time
let last_batch_time = 0.0

(* NOTE this prints the error and exits; use set_error_hook for more
   nuanced behaviour *)
let default_error_hook = fun s -> 
  Printf.printf "Fatal error: %s\n%!" s;
  Stdlib.exit (-1)

module R = Rocksdb

let open_ ~fn = R.(open_db ~config:Options.default ~name:fn) |> function
  | Ok db -> 
    Ok { db; fn; error_hook=default_error_hook; timer=Unix.time; last_batch_time=0.0 }
  | Error (`Msg e) -> 
    Error e

let create ~fn = 
  (* delete and make a new one? *)
  (try Sys.remove fn with _ -> ());
  open_ ~fn

let close t = 
  R.close_db t.db >>= function 
  | Ok () -> return ()
  | Error (`Msg e) -> 
    Printf.printf "%s\n%!" e;
    return ()

let set_error_hook t f = t.error_hook <- f

let default_write_options = R.Options.Write_options.create ()

let slow_insert t k v = R.put t.db default_write_options ~key:k ~value:v |> function
  | Ok () -> return ()
  | Error (`Msg s) -> 
    ignore(t.error_hook s);
    return ()

let slow_delete t k = R.delete t.db default_write_options k |> function
  | Ok () -> return ()
  | Error (`Msg s) -> 
    ignore(t.error_hook s);
    return ()

let default_read_options = R.Options.Read_options.create ()

let find_opt t k = R.get t.db default_read_options k >>= function
  | Ok `Not_found -> return None
  | Ok (`Found v) -> return (Some v)
  | Error (`Msg s) -> 
    ignore(t.error_hook s);
    return None

module B = R.Batch

let batch t ops =
  (* batch operations can only be puts, for some reason *)
  let t1 = t.timer () in
  let b = B.create() in  
  (ops,[]) |> iter_k (fun ~k:kont (ops,dels) -> 
      match ops with
      | [] -> dels
      | (k,`Insert v)::rest -> 
        B.put b ~key:k ~value:v;
        kont (rest,dels)
      | (k,`Delete)::rest -> 
        kont (rest,k::dels))
  |> fun dels -> 
  begin
    B.write t.db default_write_options b >>= function
    | Ok () -> return ()
    | Error (`Msg e) -> 
      ignore(t.error_hook e);
      return ()
  end >>= fun () -> 
  dels |> List.iter (slow_delete t);
  let t2 = t.timer () in
  t.last_batch_time <- t2 -. t1;
  ()

let clear t =
  close t;
  create ~fn:t.fn |> function
  | Ok x -> 
    t.db <- x.db;
    return ()
  | Error e -> 
    ignore(t.error_hook e);
    return ()

let last_batch_duration t = t.last_batch_time

let set_time t f = t.timer <- f      

let default_flush_options = R.Options.Flush_options.create ()

let sync t = R.flush t.db default_flush_options >>= function
  | Ok () -> return ()
  | Error (`Msg e) -> 
    ignore(t.error_hook e);
    return ()
    
