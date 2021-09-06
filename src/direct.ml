(*
(** Direct interface, using Lwt_preemptive *)

open Impl_intf

module Make_1 = struct
  include Impl

  type t = { 
    run_in_main : run_in_main_t;
    lwt_impl    : Impl.t
  }

  module M = struct
    type 'a t = 'a
  end

  (* For Lwt, the error case becomes an Lwt failed promise; here we
     just fail directly; no need to run_in_main! *)
  let error_to_exn e = match e with 
    | Ok x -> x
    | Error e -> failwith e

  let open_ ~(run_in_main:run_in_main_t) ~fn = 
    run_in_main.run_in_main (fun () -> open_ ~fn) |> function
    | Ok lwt_impl -> Ok { run_in_main; lwt_impl }
    | Error e -> Error e

  let create ~(run_in_main:run_in_main_t) ~fn = 
    run_in_main.run_in_main (fun () -> create ~fn) |> function
    | Ok lwt_impl -> Ok { run_in_main; lwt_impl }
    | Error e -> Error e

  let close t = t.run_in_main.run_in_main (fun () -> close t.lwt_impl)

  let slow_insert t k v = t.run_in_main.run_in_main (fun () -> slow_insert t.lwt_impl k v)

  let slow_delete t k = t.run_in_main.run_in_main (fun () -> slow_delete t.lwt_impl k)

  let find_opt t k = t.run_in_main.run_in_main (fun () -> find_opt t.lwt_impl k)
      
  let batch t ops = t.run_in_main.run_in_main (fun () -> batch t.lwt_impl ops)

  let set_error_hook t f = set_error_hook t.lwt_impl f

  let last_batch_duration t = last_batch_duration t.lwt_impl

  let set_time t f = set_time t.lwt_impl f
end

module Make_2 : Impl_intf.S_DIRECT = Make_1
*)
