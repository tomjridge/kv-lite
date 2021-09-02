let _ = ()

open Kv_lite.Private.Test

open Lwt.Infix

let () = Lwt_main.run begin
    Lwt_list.iter_s
      (fun uri -> Caqti_lwt.with_connection uri test >>= report_error)
      [(Uri.of_string "sqlite3:test.db?busy_timeout=60000")]
  end
