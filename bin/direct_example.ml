(* Direct examples: Kyoto Cabinet, Rocksdb, kv-hash, mini-btree *)

open Kv_lite.Private.Util

open Kv_lite.Private.Impl_private.With_blocking.M'

(* select implementation based on command line args *)

let impl = 
  match Sys.argv |> Array.to_list |> List.tl with
  | ["kc"] -> `KC
  | ["rocks"] -> `Rocks
  | ["hash"] -> `Kv_hash
  | ["btree"] -> `Btree
  | [] -> failwith "Need a command line arg for implementation to test"
  | [x] -> failwith (Printf.sprintf "Unrecognized command line arg: %s\n" x)
  | _::_::_ -> failwith "Too many command line args"


let m : (module Kv_lite.Impl_intf.S_DIRECT) = 
  match impl with
  | `KC -> (module Kv_lite.Kyoto_impl)
  | `Rocks -> (module Kv_lite.Rocksdb_impl)
  | `Kv_hash -> (module Kv_lite.Kv_hash_impl)
  | `Btree -> (module Kv_lite.Btree_impl)

module Kv : Kv_lite.Impl_intf.S_DIRECT = (val m)
open Kv

let slow_insert_n = 1_000

let batch_n = 1
let batch_sz = 500_000

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
    begin
      1 |> iter_k (fun ~k:kont i -> 
          match i > slow_insert_n with
          | true -> return ()
          | false -> 
            slow_insert t (string_of_int i) (string_of_int i) >>= fun () -> 
            kont (i+1))
    end;
    let t3 = time () in
    Printf.printf "%d slow inserts completed in %f\n%!" slow_insert_n (t3 -. t2);
    (* now a batch insert *)
    batch_n |> iter_k (fun ~k:kont1 n -> 
        match n > 0 with
        | false -> ()
        | true -> 
          let ops = 
            (1,[]) |> iter_k (fun ~k:kont (i,xs) -> 
                match i > batch_sz with
                | true -> xs
                | false -> 
                  let op = (string_of_int i,`Insert (string_of_int i)) in
                  kont (i+1, op::xs))
          in
          batch t ops;
          kont1 (n-1));
    let t4 = time () in
    Printf.printf "%d batches of size %d ops completed in %f\n%!" batch_n batch_sz (t4 -. t3);
    sync t >>= fun () -> 
    let t5 = time () in
    Printf.printf "Sync in %f\n%!" (t5 -. t4);
    close t >>= fun () -> 
    let t6 = time () in
    Printf.printf "Close in %f\n%!" (t6 -. t5);
    return ()
  end

let _ = go ()


