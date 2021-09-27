open Lwt
open Lwt.Infix

module Kv = Kv_lite.Impl_with_lwt
open Kv
open Kv_lite.Private.Util

let slow_insert_n = 1_000
let batch_n = 100_000

let time () = Unix.time ()

let go () = 
  begin
    Printf.printf "Test starts\n%!";
    let t1 = time () in
    let fn = "test.db" in
    (* open db; create drops any existing db *)
    Kv.create ~fn >>= Kv.error_to_exn >>= fun t -> 
    Gc.full_major ();
    let t2 = time () in
    Printf.printf "Create completed in %f\n%!" (t2 -. t1);
    (* add some entries *)
    1 |> iter_k (fun ~k:kont i -> 
        match i > slow_insert_n with
        | true -> return ()
        | false -> 
          slow_insert t (string_of_int i) (string_of_int i) >>= fun () -> 
          kont (i+1))
    >>= fun () -> 
    Gc.full_major ();
    let t3 = time () in
    Printf.printf "%d slow inserts completed in %f\n%!" slow_insert_n (t3 -. t2);
    (* now a batch insert *)
    let ops = 
      (1,[]) |> iter_k (fun ~k:kont (i,xs) -> 
          match i > batch_n with
          | true -> xs
          | false -> 
            let op = (string_of_int i,`Insert (string_of_int i)) in
            kont (i+1, op::xs))
    in
    batch t ops >>= fun () -> 
    Gc.full_major ();
    let t4 = time () in
    Printf.printf "%d batch ops completed in %f\n%!" batch_n (t4 -. t3);
    close t >>= fun () -> 
    return ()
  end

let _ = Lwt_main.run (go ())


(* Timings:

time dune exec bin/example.exe
Test starts
Create completed in 0.000000
1000 slow inserts completed in 4.000000
100000 batch ops completed in 1.000000

real	0m5.886s
user	0m1.376s
sys	0m1.074s

*)


