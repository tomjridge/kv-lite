(** Main interfaces to [kv-lite] *)

(** {1 Lwt instance} *)

module Impl_intf = Impl_intf

module type S_LWT = Impl_intf.S_LWT

include Impl


(** {1 Direct-style instance} *)

(** This is a direct-style interface, assuming Lwt main is running in
   another system thread. NOTE the ocamldoc is not very good - type
   ['a M.t] here is just ['a] *)
module Direct = Direct.Make_2


(** {1 Private modules} *)

module Private = struct
  module Util = Util
  module Test = Test
end
