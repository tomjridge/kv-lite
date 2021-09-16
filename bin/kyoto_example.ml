(* Example using Kyoto Cabinet as the backend *)

open Kv_lite.Private.Util

(* open Kv_lite.Private.Impl_private.With_blocking *)
open Kv_lite.Private.Impl_private.With_blocking.M'

module Kv = Kv_lite.Kyoto_impl
open Kv

let slow_insert_n = 5_000_000
let batch_n = 10_000
let n_batch = 0

let time () = Unix.time ()

let go () = 
  begin
    Printf.printf "Test starts\n%!";
    let t1 = time () in
    let fn = "test.db" in
    (* open db; create drops any existing db *)
    Kv.create ~fn >>= Kv.error_to_exn >>= fun t -> 
    let t2 = time () in
    Printf.printf "Create completed in %f\n%!" (t2 -. t1);
    (* add some entries *)
    1 |> iter_k (fun ~k:kont i -> 
        match i > slow_insert_n with
        | true -> return ()
        | false -> 
          let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
          slow_insert t (string_of_int k) (string_of_int k) >>= fun () -> 
          kont (i+1))
    >>= fun () -> 
    let t3 = time () in
    Printf.printf "%d slow inserts completed in %f\n%!" slow_insert_n (t3 -. t2);
    (* now do n_batch batch inserts, each of size batch_n; kyotoc
       doesn't have batch operations, so quickest to just execute the
       operations immediately *)
    0 |> iter_k (fun ~k:kont1 m -> 
        match m >= n_batch with 
        | true -> ()
        | false -> 
          begin
            let ops = 
              (1,[]) |> iter_k (fun ~k:kont (i,xs) -> 
                  match i > batch_n with
                  | true -> xs
                  | false -> 
                    let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
                    let op = (string_of_int k,`Insert (string_of_int k)) in
                    kont (i+1, op::xs))
            in
            batch t ops
          end >>= fun () -> 
          kont1 (m+1))
    >>= fun () -> 
    let t4 = time () in
    Printf.printf "%d batch ops completed in %f\n%!" (n_batch * batch_n) (t4 -. t3);
    sync t >>= fun () -> 
    let t5 = time () in
    Printf.printf "Sync in %f\n%!" (t5 -. t4);
    close t >>= fun () -> 
    let t6 = time () in
    Printf.printf "Close in %f\n%!" (t6 -. t5);
    return ()
  end

let _ = go ()


(* Timings:


*)



