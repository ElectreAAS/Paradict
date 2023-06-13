(lang dune 3.8)

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
  (ocaml
   (>= "5.0.0"))
  (kcas
   (>= "0.5.2"))
  (ocamlformat
   (and
    :with-test
    (= "0.25.1")))
  (domainslib :with-test)
  (mtime
   (and
    :with-test
    (>= "2.0.0")))
  (alcotest :with-test)
  (bisect_ppx :with-test))
 (tags
  (topics "to describe" your project)))
