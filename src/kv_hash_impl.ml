(** Blocking implementation using kv-hash *)

open Util

module Make_1 = struct
  module Kv = Kv_hash.String_string_map

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

  let open_ ~fn:_ = failwith "FIXME"

  let create' ~fn = 
    Kv.create ~fn |> fun db -> 
    { db; fn; error_hook=default_error_hook;timer;last_batch_time }

  let create ~fn = create' ~fn |> fun x -> Ok x

  let close t = Kv.close t.db

  let set_error_hook t f = t.error_hook <- f

  let slow_insert t k v = Kv.insert t.db k v

  let slow_delete t k = Kv.delete t.db k

  let find_opt t k = Kv.find_opt t.db k

  let batch t ops =
    (* batch operations can only be puts, for some reason *)
    let t1 = t.timer () in
    ops |> iter_k (fun ~k:kont ops -> 
        match ops with
        | [] -> ()
        | (k,`Insert v)::rest -> 
          slow_insert t k v;
          kont rest
        | (k,`Delete)::rest -> 
          slow_delete t k;
          kont rest)
    |> fun () ->
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

end

module Make_2 : Impl_intf.S_DIRECT = Make_1

include Make_1
