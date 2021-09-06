(** Direct interface, using Caqti blocking interface *)

open Util


module Make_1 = struct
  module M = struct
    type 'a t = 'a
  end

  type op = Impl_private.op

  type 'a or_error = 'a Impl_private.or_error

  type t = {
    conn: (module Caqti_blocking.CONNECTION);
    mutable error_hook: (string -> unit);
    mutable timer: (unit -> float);
    mutable last_batch_time: float;
  }

  let table,with_table = Impl_private.(table,with_table)

  module Q = Impl_private.Q

  (* NOTE this prints the error and exits; use set_error_hook for more
     nuanced behaviour *)
  let default_error_hook = fun s -> 
    Printf.printf "Fatal error: %s\n%!" s;
    Stdlib.exit (-1)

  module Caqti_bind = struct
    let (>>=?) m f =
      m |> (function | Ok x -> f x | Error err -> (Error err))
  end

  let ( >>= ) m f = m |> f

  let return x = x

  let return_caqti_error e = return (Error (Caqti_error.show e))

  module Caqti_monad = Caqti_blocking

  let create ~fn =
    let open Caqti_bind in
    let uri = Uri.of_string @@ "sqlite3:"^fn^"?busy_timeout=60000" in
    begin
      Caqti_blocking.connect uri >>=? fun conn -> 
      let (module Db : Caqti_blocking.CONNECTION) = conn in
      Db.find Q.wal_mode () >>=? fun _ -> 
      Db.exec Q.drop () >>=? fun () -> 
      Db.exec Q.create () >>=? fun () -> 
      return @@ Ok {
        conn;
        error_hook=default_error_hook;
        timer=Sys.time;
        last_batch_time=0.0
      }
    end >>= function
    | Ok e -> return (Ok e)
    | Error e -> return_caqti_error e

  (* open is OK to use on a non-existent db *)
  let open_ ~fn = 
    let open Caqti_bind in
    let uri = Uri.of_string @@ "sqlite3:"^fn^"?busy_timeout=60000" in
    begin
      Caqti_monad.connect uri >>=? fun conn -> 
      let (module Db : Caqti_monad.CONNECTION) = conn in
      Db.find Q.wal_mode () >>=? fun _ -> 
      Db.exec Q.create () >>=? fun () -> 
      return (Ok {
          conn;
          error_hook=default_error_hook;
          timer=Sys.time;
          last_batch_time=0.0
        })
    end >>= function 
    | Ok e -> return (Ok e)
    | Error e -> return_caqti_error e
                   

  let close t =
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    Db.disconnect ()

  let set_error_hook t f = t.error_hook <- f


  let slow_insert (t:t) k v =
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    Db.exec Q.insert (k, v) >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x

  let slow_delete t k =
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    Db.exec Q.delete k >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x

  let find_opt t k  =
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    Db.find_opt Q.find_opt k >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return None
    | Ok x -> return x

  let clear t = 
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    Db.exec Q.clear () >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x


  (* FIXME this should use the error hook rather than returning a result
  *)
  let batch t (ops: op list) =
    let open Caqti_bind in
    let (module Db : Caqti_monad.CONNECTION) = t.conn in
    let t1 = t.timer () in
    begin
      Db.start () >>=? fun () -> 
      begin 
        ops |> iter_k (fun ~k:kont ops -> 
            match ops with
            | [] -> return (Ok())
            | (k,`Insert v)::rest -> 
              Db.exec Q.insert (k,v) >>=? fun () -> 
              kont rest
            | (k, `Delete)::rest -> 
              Db.exec Q.delete k >>=? fun () -> 
              kont rest)
      end >>=? fun () -> 
      Db.commit ()
    end
    >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok () -> 
      let t2 = t.timer () in
      t.last_batch_time <- t2 -. t1;
      return ()

  let _ = batch

  let last_batch_duration t = t.last_batch_time

  let set_time t f = t.timer <- f      

  let error_to_exn (x:'a or_error) = 
    match x with
    | Ok x -> return x
    | Error e -> failwith e

end

module Make_2 : Impl_intf.S_DIRECT = Make_1

