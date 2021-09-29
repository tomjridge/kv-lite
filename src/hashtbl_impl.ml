(** A test oracle that just performs batch operations and can wrap an
   underlying KV instance. *)

open Util
open Impl_intf

module Make(S:sig type k type v end) = struct
  open S
  type t = {
    tbl: (k,v)Hashtbl.t;
    mutable log: (k,v)op list; (* all ops, held in reverse *)
  }

  let open_ () = {
    tbl=Hashtbl.create 1024;
    log=[];
  }

  let batch t ops = 
    let per_op = function
      | (k,`Delete) as op -> 
        Hashtbl.remove t.tbl k;
        t.log <- op::t.log;
        ()
      | (k,`Insert v) as op -> 
        Hashtbl.replace t.tbl k v;
        t.log <- op::t.log;
        ()
    in
    List.iter per_op ops

  let find_opt t k = Hashtbl.find_opt t.tbl k

  let clear t = Hashtbl.clear t.tbl

  let slow_insert t k v = batch t [(k,`Insert v)]

  let slow_delete t k = batch t [(k,`Delete)]

end


module Wrap_1(Base:S_DIRECT) = struct
  type k = string
  type v = string

  module Tbl = Make(struct type k=string type v=string end)
      
  
  
  type t = { 
    base:Base.t; 
    tbl: Tbl.t; 
    lock: Mutex.t;
    mutable error_hook: 
      key:k -> value:v option -> expected:v option -> log:(string,string) op list -> unit
  }


  let error_hook ~key ~value ~expected ~(log:(string,string)op list) = 
    Printf.printf "%s: mismatch between base and test oracle\n%!" __MODULE__;
    Printf.printf "Key was %S; value was %S; expected was %S\n%!" 
      key
      (if value=None then "None" else Option.get value)
      (if expected=None then "None" else Option.get expected);
    1 |> Util.iter_k (fun ~k i -> 
        let fn = ("/tmp/log_"^(string_of_int i)) in
        match Sys.file_exists fn with
        | true -> k (i+1)
        | false -> fn) |> fun fn ->     
    Trace.write fn (List.rev_map (function | (k,`Insert v) -> (k,`Insert v) | (k,`Delete) -> (k,`Delete)) log);
    Printf.printf "Log written to file %s\n%!" fn;
    ()
    
  let open_ ~fn = 
    Base.open_ ~fn |> fun base -> 
    assert(Result.is_ok base);
    Ok { 
      base=(Result.get_ok base);
      tbl=Tbl.open_ ();
      lock=Mutex.create();      
      error_hook 
    }

  let with_lock lock f = 
    Mutex.lock lock;
    try 
      let x = f() in
      Mutex.unlock lock;
      x
    with e ->
      Mutex.unlock lock;
      raise e

  let set_error_hook t error_hook = t.error_hook <- error_hook

  let batch t ops = 
    Base.batch t.base ops;
    Tbl.batch t.tbl ops;
    ()

  let batch t ops = with_lock t.lock (fun () -> batch t ops)
    
  let close t = 
    Printf.printf "WARNING!!! close called on %s\n%!" __MODULE__;
    Base.close t.base;
    ()
      
  let clear t = 
    Printf.printf "WARNING!!! clear called on %s\n%!" __MODULE__;
    Base.clear t.base;
    Tbl.clear t.tbl;
    ()

  let sync t =
    Printf.printf "WARNING!!! sync called on %s\n%!" __MODULE__;
    Base.sync t.base;
    ()
    
  let find_opt t k =
    Base.find_opt t.base k |> fun v -> 
    let expected = Tbl.find_opt t.tbl k in
    ignore(v = expected || (t.error_hook ~key:k ~value:v ~expected ~log:t.tbl.log;false)); (* FIXME ignoring for now but should check; possible concurrency bug when using hashtbl concurrently? *)
    v

  let find_opt t k = with_lock t.lock (fun () -> find_opt t k)
    
end

module type S = sig
  type k = string
  type v = string
  type t 
  val open_ : fn:v -> (t, 'a) result
  val batch : t -> (k,v) op list -> unit
  val close : t -> unit
  val clear : t -> unit
  val sync : t -> unit
  val find_opt : t -> v -> v option
  val set_error_hook :
    t -> (key:v -> value:v option -> expected:v option -> log:(string,string)op list -> unit) -> unit
end

module Wrap(Base:S_DIRECT) : S = Wrap_1(Base)
