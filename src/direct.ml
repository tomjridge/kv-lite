(** Direct interface, using Lwt_preemptive *)

(** We make the user provide [run_in_main] so that they are forced to
   consider where the Lwt main is actually running (hopefully in a
   separate system thread). *)
module type S = sig val run_in_main: (unit -> 'a Lwt.t) -> 'a end

module Make_1(S:S) = struct
  open S

  include Impl

  module M = struct
    type 'a t = 'a
  end

  (* For Lwt, the error case becomes an Lwt failed promise; here we
     just fail directly; no need to run_in_main! *)
  let error_to_exn e = match e with 
    | Ok x -> x
    | Error e -> failwith e

  let open_ ~fn = run_in_main (fun () -> open_ ~fn)

  let create ~fn = run_in_main (fun () -> create ~fn)

  let close t = run_in_main (fun () -> close t)

  let slow_insert t k v = run_in_main (fun () -> slow_insert t k v)

  let slow_delete t k = run_in_main (fun () -> slow_delete t k)

  let find_opt t k = run_in_main (fun () -> find_opt t k)
      
  let batch t ops = run_in_main (fun () -> batch t ops)

end

module Make_2(S:S) : Impl_intf.S_DIRECT = Make_1(S)
