module type S = Impl_intf.S

include (Impl : S)

module Private = struct
  module Test = Test
end
