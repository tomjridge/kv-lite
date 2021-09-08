(** Blocking implementation using kyotocabinet *)

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
  mutable db              : Kyoto.db; 
  fn                      : string; 
  mutable timer           : (unit -> float); 
  mutable last_batch_time : float 
}

let timer = Sys.time
let last_batch_time = 0.0

let add_params fn = fn ^ "#type=kch" (* or kct for tree *)

let default_modes = [] (* Kyoto.[OAUTOTRAN;OAUTOSYNC] *)

let open' fn = Kyoto.(opendb (add_params fn) (default_modes@[OREADER;OWRITER;OCREATE]))

let open_ ~fn = 
  try 
    open' fn |> fun db -> 
    Ok { db; fn; timer; last_batch_time }
  with e -> 
    Error (Printexc.to_string e)

let create' fn = 
  Kyoto.(opendb (add_params fn) (default_modes@[OREADER;OWRITER;OCREATE;OTRUNCATE]))

let create ~fn = 
  try 
    create' fn |> fun db -> 
    Ok { db; fn; timer; last_batch_time }
  with e -> 
    Error (Printexc.to_string e)

let close t = Kyoto.close t.db

let set_error_hook _t _f = ()

(* FIXME for kyoto, these are not slow *)
let slow_insert t k v = Kyoto.set t.db k v

let slow_delete t k = Kyoto.remove t.db k

let find_opt t k = Kyoto.get t.db k

(* NOTE Kyoto transactions slow things down considerably; in default
   mode, a process or system crash may cause some records to be
   missing, and auto recovery is proportional to db size :( See here:
   https://dbmx.net/kyotocabinet/spex.html *)
let batch t ops = 
  let t1 = t.timer () in
  (* Kyoto.begin_tran_sync t.db; *)
  ops |> List.iter (function
      | (k,`Insert v) -> slow_insert t k v
      | (k,`Delete) -> slow_delete t k);
  let t2 = t.timer () in
  (* Kyoto.commit_tran t.db; *)
  t.last_batch_time <- t2 -. t1;
  ()

let clear t = 
  close t;
  (* FIXME check for error here? *)
  create' t.fn |> fun db -> 
  t.db <- db

let set_time t timer = t.timer <- timer
  
let last_batch_duration t = t.last_batch_time  

let sync t =
  close t;
  open' t.fn |> fun db -> 
  t.db <- db

