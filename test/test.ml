open Paradict.Make (struct
  type t = string

  let compare = compare
  let to_string x = x
end)

let mem_empty =
  Alcotest.test_case "mem on empty map" `Quick @@ fun () ->
  let scores = create () in
  let res = mem "Among us" scores in
  Alcotest.(check bool) "Unknown entry shouldn't be found" false res;
  ()

let add_mem =
  Alcotest.test_case "add & mem" `Quick @@ fun () ->
  let scores = create () in
  add "Factorio" 10 scores;
  let res = mem "Factorio" scores in
  Alcotest.(check bool) "Added entry should be found" true res;
  ()

let add_find =
  Alcotest.test_case "add & find_opt" `Quick @@ fun () ->
  let scores = create () in
  add "Sekiro" 9 scores;
  let res = find_opt "Sekiro" scores in
  Alcotest.(check (option int))
    "Added entry should have correct value" (Some 9) res;
  ()

let remove_empty =
  Alcotest.test_case "remove on empty map" `Quick @@ fun () ->
  let scores = create () in
  let removed = remove "Dead Cells" scores in
  Alcotest.(check bool) "Removing an unknown key should fail" false removed;
  ()

let add_remove =
  Alcotest.test_case "add & remove" `Quick @@ fun () ->
  let scores = create () in
  add "Portal" 7 scores;
  let removed = remove "Portal" scores in
  Alcotest.(check bool) "Removing a known key should succeed" true removed;
  ()

let basics = [ mem_empty; add_mem; add_find; remove_empty; add_remove ]

let collision_mem =
  Alcotest.test_case "collision & mem & depth" `Quick @@ fun () ->
  let numbers = create () in
  for i = 0 to 32 do
    add (string_of_int i) i numbers
  done;
  Alcotest.(check bool)
    "After a row has been filled, depth should be at least 2" true
    (depth numbers >= 2);
  for i = 0 to 32 do
    let found = find_opt (string_of_int i) numbers in
    Alcotest.(check (option int))
      (string_of_int i ^ " should be found even in case of collision")
      (Some i) found
  done;
  ()

let collisions = [ collision_mem ]

let () =
  Alcotest.run "Sequential operations"
    [ ("Basic operations", basics); ("Collisions", collisions) ]
