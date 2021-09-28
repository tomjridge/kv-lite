(** Replay a trace of operations *)

let fn = Sys.argv|>Array.to_list|>List.tl |> function
  | [fn] -> fn
  | _ -> failwith "Command line args"


module Kv' = Kv_lite.Btree_impl
module Kv = Kv_lite.Hashtbl_impl.Wrap(Kv')

let ops = Kv_lite.Private.Trace.read fn

let main () = 
  Printf.printf "Replaying trace...\n%!";
  Kv.open_ ~fn:"replay_test" |> Result.get_ok |> fun t -> 
  let per_op op = 
    match op with
    | (k, `Insert v) -> Kv.batch t [(k,`Insert v)]
    | (k, `Delete) -> Kv.batch t [(k,`Delete)]
    | (k, `Find v) -> assert(v = Kv.find_opt t k)
  in
  ops |> List.iter per_op

let _ = main ()
  
