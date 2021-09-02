open Lwt.Infix
open Util

module Dynparam = struct
  type t = Pack : 'a Caqti_type.t * 'a -> t
  let empty = Pack (Caqti_type.unit, ())
  let add t x (Pack (t', x')) = Pack (Caqti_type.tup2 t' t, (x', x))
end

module Q = struct

  let create =
    Caqti_request.exec Caqti_type.unit
      {eos|
      CREATE TABLE IF NOT EXISTS kv (
        key_ text NOT NULL,
        val text NOT NULL,
      PRIMARY KEY (key_)
      )
    |eos}
      
  let insert = 
    Caqti_request.exec
      Caqti_type.(tup2 string string)
      "INSERT OR REPLACE INTO kv VALUES (?, ?)"

  let find_opt = 
    Caqti_request.find_opt
      Caqti_type.string Caqti_type.string
      "SELECT val FROM kv WHERE key_ = ?"
      
  let delete = 
    Caqti_request.exec
      Caqti_type.string
      "DELETE FROM kv WHERE key_ = ?"

  (* see https://github.com/paurkedal/ocaml-caqti/issues/15 ; for
     transactions see
     https://github.com/paurkedal/ocaml-caqti/issues/37 ; note there
     may be -- probably are -- limits on the length of the suqery
     string *)
  let batch _ops = 
    Caqti_request.exec
      ~oneshot:true
      Caqti_type.(tup2 (tup2 string string) (tup2 string string))
      {|
INSERT OR REPLACE INTO kv (key_,val) VALUES (?,?), (?,?) 
|}
                    
end

let create (module Db : Caqti_lwt.CONNECTION) =
  Db.exec Q.create ()

[@@@warning "-32"]

let insert (module Db : Caqti_lwt.CONNECTION) k v =
  Db.exec Q.insert (k, v)

let find_opt k (module Db : Caqti_lwt.CONNECTION) =
  Db.find_opt Q.find_opt k

let delete (module Db : Caqti_lwt.CONNECTION) k =
  Db.exec Q.delete k

let (>>=?) m f =
  m >>= (function | Ok x -> f x | Error err -> Lwt.return (Error err))

(* FIXME the hope is that this executes in a single transaction *)
let batch (module Db : Caqti_lwt.CONNECTION) ops = 
  Db.start () >>=? fun () -> 
  begin 
    ops |> iter_k (fun ~k:kont ops -> 
        match ops with
        | [] -> Lwt.return_ok ()
        | (k,`Insert v)::rest -> 
          Db.exec Q.insert (k,v) >>=? fun () -> 
          kont rest
        | (k, `Delete)::rest -> 
          Db.exec Q.delete k >>=? fun () -> 
          kont rest)
  end >>= fun r ->
  Db.commit () >>= fun r' -> 
  match Result.is_error r || Result.is_error r' with
  | true -> Lwt.return (if Result.is_error r then r else r')
  | false -> Lwt.return_ok ()
  
let test db =
  (* Examples of statement execution: Create and populate the register. *)
  create db >>=? fun () ->
  insert db "k0" "v0" >>=? fun () -> 
  Printf.printf "%s\n%!" __LOC__;
  batch db [("k3",`Insert "v3");("k4",`Insert "v4")] >>=? fun () ->
  Printf.printf "%s\n%!" __LOC__;
  begin 
    (5,[]) |> iter_k (fun ~k:kont (i,ops) -> 
        match i > 100_000 with 
        | true -> ops
        | false -> 
          let k = "k"^string_of_int i in
          let v = "v"^string_of_int i in
          kont (i+1,(k,`Insert v)::ops))
    |> fun ops ->         
    batch db ops
  end >>=? fun () ->

  find_opt "k1" db >>=? fun v -> 
  begin match v with
    | None -> Printf.printf "can't find k1\n%!"
    | Some v -> Printf.printf "Found value %s\n%!" v
  end;
  find_opt "k3" db >>=? fun v -> 
  begin match v with
    | None -> failwith "can't find k3"
    | Some v -> Printf.printf "Found value %s\n%!" v
  end;
  Lwt.return (Ok ())

let report_error = function
 | Ok () -> Lwt.return_unit
 | Error err ->
    Lwt_io.eprintl (Caqti_error.show err) >|= fun () -> exit 69
