(** Blocking implementation using kv-hash *)

(* open Util *)

module Make_1 = struct
  module Kv = Kv_hash.Nv_map_ss

  module M = struct
    type 'a t = 'a
  end
  open M

  type 'a or_error = ('a,string)Stdlib.result

  let ( >>= ) m f = m |> f
  let return (x:'a) : 'a t = x

  let error_to_exn (x:'a or_error) = 
    match x with
    | Ok x -> return x
    | Error e -> failwith e

  (* NOTE for clear, we want to just reopen with a clean database *)
  type t = {
    mutable db: Kv.t;
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

  let create' ~fn = 
    Kv.create ~fn |> fun db -> 
    { db; fn; error_hook=default_error_hook;timer;last_batch_time }

  let create ~fn = create' ~fn |> fun x -> Ok x

  (* FIXME open_ should open an existing db; this is just to get
     initial perf testing working *)
  let open_ ~fn = 
    create ~fn
      
    (* failwith "FIXME kv_hash_impl: open_" *)

  let close t = Kv.close t.db

  let set_error_hook t f = t.error_hook <- f

  let slow_insert t k v = Kv.insert t.db k v

  let slow_delete _t _k = 
    Printf.printf "%s: slow_delete not supported by kv_hash" __FILE__;
    Stdlib.exit (-1)
    (* Kv.delete t.db k *)

  let find_opt t k = Kv.find_opt t.db k

  let batch t ops =
    (* batch operations can only be puts, for some reason *)
    let t1 = t.timer () in
    Kv.batch t.db ops;
    let t2 = t.timer () in
    t.last_batch_time <- t2 -. t1;
    ()

  let clear t =
    close t;
    create' ~fn:t.fn |> function t' -> 
      t.db <- t'.db;
      ()

  let last_batch_duration t = t.last_batch_time

  let set_time t f = t.timer <- f      

  let sync _t = () (* FIXME *)

end

module Make_2 : Impl_intf.S_DIRECT = Make_1

include Make_1
