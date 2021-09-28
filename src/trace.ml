(** Some utility code for traces *)

open Sexplib.Std

type op = string * [ `Insert of string | `Delete | `Find of string option ][@@deriving sexp]

type ops = op list[@@deriving sexp]

let write fn ops = 
  let oc = Stdlib.open_out_bin fn in
  Sexplib.Sexp.output_hum oc (sexp_of_ops ops);
  Stdlib.close_out_noerr oc

let read fn =
  let ic = Stdlib.open_in_bin fn in
  Sexplib.Sexp.input_sexp ic |> ops_of_sexp
  
