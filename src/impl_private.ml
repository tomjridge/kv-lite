(** kv-lite, private implementation *)


open Util

type 'a or_error = ('a,string)Stdlib.result

module type M = sig
  type +'a t
  val return: 'a -> 'a t
  val ( >>= ): 'a t -> ('a -> 'b t) -> 'b t
  val error_to_exn : 'a or_error -> 'a t
end


module Make(M:M)(Connect:Caqti_connect_sig.S with type +'a future = 'a M.t) = struct

  module M = M
  open M

  type 'a or_error = ('a,string)Stdlib.result

  type t = {
    conn: (module Connect.CONNECTION);
    mutable error_hook: (string -> unit);
    mutable timer: (unit -> float);
    mutable last_batch_time: float;
  }

  (* This should match all the following uses *)
  let table = "kv_lite"

  let with_table s = Printf.sprintf s table

  let _ = with_table

  module Q = struct

    let pragma_fus s = 
      Caqti_request.find
        Caqti_type.unit
        Caqti_type.string
        s

    (* setting wal mode returns a string, so this is a find query for caqti *)
    let wal_mode = pragma_fus "PRAGMA journal_mode=WAL"    

    (* Various unsafe speedups https://news.ycombinator.com/item?id=27872575 *)
    let unsafe_max_performance = [
      pragma_fus "PRAGMA journal_mode=OFF";
      pragma_fus "PRAGMA synchronous=O";
      pragma_fus "PRAGMA temp_store=MEMORY";
    ]

    let safe_normal = [
      pragma_fus "PRAGMA journal_mode=WAL";
      pragma_fus "PRAGMA synchronous=1";
      pragma_fus "PRAGMA temp_store=DEFAULT";
    ]    

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

    let clear = 
      Caqti_request.exec
        Caqti_type.unit
        (with_table "DELETE FROM %s")

  end

  let return_caqti_error e = return (Error (Caqti_error.show e))

  (* NOTE this prints the error and exits; use set_error_hook for more
     nuanced behaviour *)
  let default_error_hook = fun s -> 
    Printf.printf "Fatal error: %s\n%!" s;
    Stdlib.exit (-1)

  module Caqti_bind = struct
    let (>>=?) m f =
      m >>= (function | Ok x -> f x | Error err -> return (Error err))
  end

  let create ~fn =
    let open Caqti_bind in
    let uri = Uri.of_string @@ "sqlite3:"^fn^"?busy_timeout=60000" in
    begin
      Connect.connect uri >>=? fun conn -> 
      let (module Db : Connect.CONNECTION) = conn in
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
      Connect.connect uri >>=? fun conn -> 
      let (module Db : Connect.CONNECTION) = conn in
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
    let (module Db : Connect.CONNECTION) = t.conn in
    Db.disconnect ()

  let set_error_hook t f = t.error_hook <- f


  let slow_insert (t:t) k v =
    let (module Db : Connect.CONNECTION) = t.conn in
    Db.exec Q.insert (k, v) >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x

  let slow_delete t k =
    let (module Db : Connect.CONNECTION) = t.conn in
    Db.exec Q.delete k >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x

  let find_opt t k  =
    let (module Db : Connect.CONNECTION) = t.conn in
    Db.find_opt Q.find_opt k >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return None
    | Ok x -> return x

  let clear t = 
    let (module Db : Connect.CONNECTION) = t.conn in
    Db.exec Q.clear () >>= function
    | Error e -> 
      t.error_hook (Caqti_error.show e);
      return ()
    | Ok x -> return x


  (* FIXME this should use the error hook rather than returning a result
  *)
  let batch t (ops: _ op list) =
    let open Caqti_bind in
    let (module Db : Connect.CONNECTION) = t.conn in
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

  let error_to_exn = error_to_exn

  let sync _t = return ()

end  

module With_lwt = struct
  open Lwt
  module M' = struct
    type +'a t = 'a Lwt.t
    let return = return
    let ( >>= ) = ( >>= )
    let error_to_exn (x:'a or_error) = 
      match x with
      | Ok x -> return x
      | Error e -> Lwt.fail (Stdlib.Failure e)
  end
  module C = struct
    type +'a future = 'a Lwt.t
    include Caqti_lwt
  end
  include Make(M')(C)
end


module With_blocking = struct
  module M' = struct
    type +'a t = 'a 
    let ( >>= ) m f = m |> f
    let return (x:'a) : 'a t = x
    let error_to_exn (x:'a or_error) = 
      match x with
      | Ok x -> return x
      | Error e -> failwith e
  end
  module C = struct
    (* type +'a future = 'a *)
    include Caqti_blocking
  end
  include Make(M')(C)
end
  
