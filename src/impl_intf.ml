(** Interface to kv-lite *)

(** Generic over monad *)
module type S_GENERIC = sig

  module M : sig
    type 'a t
  end

  (** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
  type op = string * [ `Insert of string | `Delete ]

  type 'a or_error = ('a,string)Result.t

  (** This unsafe function allows you to discard the error case; an
     exception will be raised at runtime; only use for testing! *)
  val error_to_exn : 'a or_error -> 'a M.t


  (** runtime db connection; error hook; error flag *)
  type t


  (** disconnect cannot throw an error in caqti *)
  val close : t -> unit M.t


  (** Hook to catch post-initialization pre-close errors, which we
     hope do not occur; this allows the main insert/delete operations
     to avoid returning a result type. By default, this will exit the
     process, so be sure to set this in production code.   *)
  val set_error_hook : t -> (string -> unit) -> unit

  (** Following single operations are slow -- they execute as a single
     transaction -- so use batch operations if possible *)

  (** [slow_insert t k v] *)
  val slow_insert: t -> string -> string -> unit M.t

  (** [slow_delete t k v] *)
  val slow_delete: t -> string -> unit M.t

  (** [find_opt t k] *)
  val find_opt: t -> string -> string option M.t

  (** following is fast because it batches multiple operations into a
     single transaction *)
  val batch: t -> op list -> unit M.t

  (** for profiling *)
  val last_batch_duration: t -> float

  (** update the standard timer (Sys.time) with a possibly more accurate one *)
  val set_time: t -> (unit -> float) -> unit

end

(** Lwt interface - our standard interface *)
module type S_LWT = 
sig
  include S_GENERIC 

  (** Error string if the db fails to open for some reason; kv-lite
      expects to use a lone database over which it has complete
      control; the default table name is "kv_lite" just in case you
      need it *)
  val open_ : fn:string -> t or_error M.t

  val create : fn:string -> t or_error M.t
end with type 'a M.t = 'a Lwt.t


type run_in_main_t = {
  run_in_main : 'a. (unit -> 'a Lwt.t) -> 'a;
}

(** Direct interface *)
module type S_DIRECT = 
sig
  include S_GENERIC 

  (** Error string if the db fails to open for some reason; kv-lite
      expects to use a lone database over which it has complete
      control; the default table name is "kv_lite" just in case you
      need it *)
  val open_ : run_in_main:run_in_main_t -> fn:string -> t or_error M.t

  val create : run_in_main:run_in_main_t -> fn:string -> t or_error M.t      

end with type 'a M.t = 'a
