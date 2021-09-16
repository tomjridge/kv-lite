(** Main interfaces to [kv-lite] *)

module Impl_intf = Impl_intf

module type S_GENERIC = Impl_intf.S_GENERIC

(** {1 Lwt instance} *)

module Impl_with_lwt = Impl_with_lwt

(** {1 Direct-style instance} *)

module Impl_direct = Impl_direct

(** {1 Kyoto cabinet backend} *)

module Kyoto_impl = (Kyoto_impl : Impl_intf.S_DIRECT)

(** {1 Rocksdb backend} *)

module Rocksdb_impl = (Rocksdb_impl : Impl_intf.S_DIRECT)

(** {1 Kv-hash backend} *)

module Kv_hash_impl = (Kv_hash_impl : Impl_intf.S_DIRECT)

(** {1 Private modules} *)

module Private = struct
  module Util = Util
  module Test = Test
  module Impl_private = Impl_private
end
