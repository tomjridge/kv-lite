open Lwt
open Lwt.Infix

module Kv = Kv_lite
open Kv_lite
open Private.Util

let slow_insert_n = 1_000
let batch_n = 100_000

let go () = 
  begin
    Printf.printf "Test starts\n%!";
    let t1 = Sys.time () in
    let fn = "test.db" in
    (* open db; create drops any existing db *)
    Kv.create ~fn >>= Kv.error_to_exn >>= fun t -> 
    Gc.full_major ();
    let t2 = Sys.time () in
    Printf.printf "Create completed in %f\n%!" (t2 -. t1);
    (* add some entries *)
    let t1 = Sys.time () in
    1 |> iter_k (fun ~k:kont i -> 
        match i > slow_insert_n with
        | true -> return ()
        | false -> 
          slow_insert t (string_of_int i) (string_of_int i) >>= fun () -> 
          kont (i+1))
    >>= fun () -> 
    Gc.full_major ();
    let t2 = Sys.time () in
    Printf.printf "%d slow inserts completed in %f\n%!" slow_insert_n (t2 -. t1);
    (* now a batch insert *)
    let t1 = Sys.time () in
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
    let t2 = Sys.time () in
    Printf.printf "%d batch ops completed in %f\n%!" batch_n (t2 -. t1);
    close t >>= fun () -> 
    return ()
  end

let _ = Lwt_main.run (go ())


(* Timings:

time dune exec main
Test starts
Create completed in 0.004363
1000 slow inserts completed in 1.093116
100000 batch ops completed in 1.407928

real	0m12.821s
user	0m1.189s
sys	0m1.381s

NOTE The real time doesn't seem to reflect the individual times. The
slow inserts seem to take the extra wall clock time. Is it possible
that Caqti is detaching these and returning before they complete?

*)
