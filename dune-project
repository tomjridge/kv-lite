(lang dune 2.7)
(name kv_lite)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/FIXME))
(homepage             https://github.com/tomjridge/FIXME)
(documentation        https://tomjridge.github.io/FIXME/ocamldoc/index.html)
(generate_opam_files  true)


(package 
 (name kv_lite)
 (synopsis "A simple Key Value store")
 (depends lwt caqti caqti-lwt caqti-driver-sqlite3)) ; caqti-dynload? seems to cause problems with findlib in later packages - mirage/index for example


