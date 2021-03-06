(** Implementation interfaces *)

open Util

(** Generic over monad *)
module type S_GENERIC = sig

  module M : sig
    type 'a t
  end

  (** This unsafe function allows you to discard the error case; an
     exception will be raised at runtime; only use for testing! *)
  val error_to_exn : 'a or_error -> 'a M.t


  (** runtime db connection; error hook; error flag *)
  type t


  (** Error string if the db fails to open for some reason; kv-lite
      expects to use a lone database over which it has complete
      control; the default table name is "kv_lite" just in case you
      need it *)
  val open_ : fn:string -> t or_error M.t

  val create : fn:string -> t or_error M.t


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
  val batch: t -> (string,string)op list -> unit M.t

  val clear: t -> unit M.t

  (** for profiling *)
  val last_batch_duration: t -> float

  (** update the standard timer (Sys.time) with a possibly more accurate one *)
  val set_time: t -> (unit -> float) -> unit

  val sync: t -> unit M.t

end

(** Lwt interface - our standard interface *)
module type S_LWT = S_GENERIC with type 'a M.t = 'a Lwt.t

(** Direct interface *)
module type S_DIRECT = S_GENERIC with type 'a M.t = 'a

