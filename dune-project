(lang dune 3.3)

(name paradict)

(generate_opam_files true)

(source
 (github ElectreAAS/paradict))

(authors "Ambre Austen Suhamy")

(maintainers "Ambre Austen Suhamy")

(license GPL-3.0-only)

(package
 (name paradict)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
  ocaml
  dune
  ocaml_intrinsics
  digestif
  (kcas (= dev))
  (domainslib :with-test)
  (mtime :with-test)
  (alcotest :with-test)
  (bisect_ppx :with-test))
 (tags
  (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
