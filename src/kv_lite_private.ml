(** kv-lite, private implementation *)


open Lwt
open Lwt.Infix
open Util

(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

type 'a or_error = ('a,string)Result.t

type t = {
  conn: (module Caqti_lwt.CONNECTION);
  mutable error_hook: (string -> unit);
  mutable timer: (unit -> float);
  mutable last_batch_time: float;
}

(* This should match all the following uses *)
let table = "kv_lite"

let with_table s = Printf.sprintf s table

let _ = with_table

module Q = struct

  let drop =
    Caqti_request.exec Caqti_type.unit
      (with_table "DROP TABLE IF EXISTS %s")

  let create =
    Caqti_request.exec Caqti_type.unit
      (with_table {|
      CREATE TABLE IF NOT EXISTS %s (
        key_ text NOT NULL,
        val text NOT NULL,
      PRIMARY KEY (key_)
      )
    |})

  let insert = 
    Caqti_request.exec
      Caqti_type.(tup2 string string)
      (with_table "INSERT OR REPLACE INTO %s VALUES (?, ?)")

  let find_opt = 
    Caqti_request.find_opt
      Caqti_type.string Caqti_type.string
      (with_table "SELECT val FROM %s WHERE key_ = ?")
      
  let delete = 
    Caqti_request.exec
      Caqti_type.string
      (with_table "DELETE FROM %s WHERE key_ = ?")
                    
end

let return_error e = return (Error (Caqti_error.show e))

(* This stops everything; use set_error_hook for more nuanced behaviour *)
let default_error_hook = fun s -> 
  Printf.printf "Fatal error: %s\n%!" s;
  Stdlib.exit (-1)

(* FIXME this should fail if the table already exists, or if the file exists *)
let create ~fn =
  let uri = Uri.of_string @@ "sqlite3:"^fn^"?busy_timeout=60000" in
  Caqti_lwt.connect uri >>= function
  | Error e -> return_error e
  | Ok conn -> 
    let (module Db : Caqti_lwt.CONNECTION) = conn in
    Db.exec Q.drop () >>= function
    | Error e -> return_error e
    | Ok () -> 
      Db.exec Q.create () >>= function
    | Error e -> return_error e
    | Ok () -> 
      return @@ Ok {
        conn;
        error_hook=default_error_hook;
        timer=Sys.time;
        last_batch_time=0.0
      }

(* open is OK to use on a non-existent db *)
let open_ ~fn = 
  let uri = Uri.of_string @@ "sqlite3:"^fn^"?busy_timeout=60000" in
  Caqti_lwt.connect uri >>= function
  | Error e -> return_error e
  | Ok conn -> 
    let (module Db : Caqti_lwt.CONNECTION) = conn in
    Db.exec Q.create () >>= function
    | Error e -> return_error e
    | Ok () -> 
      return @@ Ok {
        conn;
        error_hook=(fun s -> 
            Printf.printf "Fatal error: %s\n%!" s);
        timer=Sys.time;
        last_batch_time=0.0
      }

let close t =
  let (module Db : Caqti_lwt.CONNECTION) = t.conn in
  Db.disconnect ()

let set_error_hook t f = t.error_hook <- f


let (>>=?) m f =
  m >>= (function | Ok x -> f x | Error err -> return_error err)

let slow_insert (t:t) k v =
  let (module Db : Caqti_lwt.CONNECTION) = t.conn in
  Db.exec Q.insert (k, v) >>= function
  | Error e -> 
    t.error_hook (Caqti_error.show e);
    return ()
  | Ok x -> return x

let slow_delete t k =
  let (module Db : Caqti_lwt.CONNECTION) = t.conn in
  Db.exec Q.delete k >>= function
  | Error e -> 
    t.error_hook (Caqti_error.show e);
    return ()
  | Ok x -> return x


let find_opt t k  =
  let (module Db : Caqti_lwt.CONNECTION) = t.conn in
  Db.find_opt Q.find_opt k >>= function
  | Error e -> 
    t.error_hook (Caqti_error.show e);
    return None
  | Ok x -> return x

let batch t ops =
  let (module Db : Caqti_lwt.CONNECTION) = t.conn in
  let t1 = t.timer () in
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
  end >>= function
  | Error e -> return (Error e)
  | Ok () -> 
    Db.commit () >>=? fun () -> 
    let t2 = t.timer () in
    t.last_batch_time <- t2 -. t1;
    Lwt.return_ok ()

let last_batch_time t = t.last_batch_time

let set_time t f = t.timer <- f      

let error_to_exn (x:'a or_error) = 
  match x with
  | Ok x -> return x
  | Error e -> failwith e
  

