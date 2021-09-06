module type S_LWT = Impl_intf.S_LWT

include (Impl : S_LWT)

(** The following constructs a direct-style interface, assuming Lwt
   main is running in another system thread *)
module Make_direct = Direct.Make_2

module Private = struct
  module Util = Util
  module Test = Test
end
