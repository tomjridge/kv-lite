(** A test oracle that just performs batch operations and can wrap an
   underlying KV instance. *)

open Impl_intf

module Make(S:sig type k type v end) = struct
  open S
  type t = (k,v)Hashtbl.t

  let open_ () = Hashtbl.create 1024

  let batch t ops = 
    let per_op = function
      | (k,`Delete) -> Hashtbl.remove t k
      | (k,`Insert v) -> Hashtbl.replace t k v
    in
    List.iter per_op ops

  let find_opt t k = Hashtbl.find_opt t k

  let clear t = Hashtbl.clear t

  let slow_insert t k v = batch t [(k,`Insert v)]

  let slow_delete t k = batch t [(k,`Delete)]

end


module Wrap_1(Base:S_DIRECT) = struct
  type k = string
  type v = string

  module Tbl = Make(struct type k=string type v=string end)
      
  type t = { base:Base.t; tbl: Tbl.t }

  let open_ ~fn = 
    Base.open_ ~fn |> fun base -> 
    assert(Result.is_ok base);
    Ok { base=(Result.get_ok base);tbl=Tbl.open_ () }

  let batch t ops = 
    Base.batch t.base ops;
    Tbl.batch t.tbl ops;
    ()
    
  let close t = 
    Base.close t.base;
    ()
      
  let clear t = 
    Base.clear t.base;
    Tbl.clear t.tbl;
    ()

  let sync t =
    Base.sync t.base;
    ()

  let find_opt t k =
    Base.find_opt t.base k |> fun v -> 
    assert(v = Tbl.find_opt t.tbl k ||
           begin 
             Printf.printf "%s: mismatch between base and test oracle\n%!" __MODULE__;
             Printf.printf "Key was %s; value was %s \n%!" 
               k 
               (if v=None then "None" else Option.get v);             
             false
           end
          );
    v
end

(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type ('k,'v) op = 'k * [ `Insert of 'v | `Delete ]

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
end

module Wrap(Base:S_DIRECT) : S = Wrap_1(Base)
