module type S = Impl_intf.S

include (Impl : S)

module Private = struct
  module Util = Util
  module Test = Test
end
