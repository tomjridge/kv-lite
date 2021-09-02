(** Interface to kv-lite *)

module type S = sig

  (** runtime db connection; error hook; error flag *)
  type t 

  type 'a or_error = ('a,string)Result.t


  (** This unsafe function allows you to discard the error case; an
     exception will be raised at runtime; only use for testing! *)
  val unsafe_ignore_error : t or_error -> t Lwt.t

  (** Error string if the db fails to open for some reason *)
  val open_ : fn:string -> t or_error Lwt.t

  val create : fn:string -> t or_error Lwt.t

  val close : t -> unit or_error Lwt.t


  (** Hook to catch post-initialization pre-close errors, which we
     hope do not occur; this allows the main insert/delete operations
     to avoid returning a result type *)
  val set_error_hook : (string -> unit) -> unit

  (** Following single operations are slow -- they execute as a single
     transaction -- so use batch operations if possible *)

  (** [slow_insert t k v] *)
  val slow_insert: t -> string -> string -> unit Lwt.t

  (** [slow_delete t k v] *)
  val slow_delete: t -> string -> unit Lwt.t

  (** [find_opt t k] *)
  val find_opt: t -> string -> string option Lwt.t  

  (** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
  type op = string * [ `Insert of string | `Delete ]

  (** following is fast because it batches multiple operations into a
     single transaction *)
  val batch: op list -> unit Lwt.t

end
