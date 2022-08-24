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

let basics = [ mem_empty; add_mem; add_find ]

let collision_mem =
  Alcotest.test_case "collision & mem" `Quick @@ fun () ->
  let numbers = create () in
  for i = 0 to 32 do
    add (string_of_int i) i numbers
  done;
  for i = 0 to 32 do
    let found = find_opt (string_of_int i) numbers in
    Alcotest.(check (option int))
      (string_of_int i ^ " should be found even in case of collision")
      (Some i) found
  done;
  ()

let depth_full_empty =
  Alcotest.test_case "size after full remove" `Quick @@ fun () ->
  let numbers = create () in
  for i = 0 to 32 do
    add (string_of_int i) i numbers
  done;
  for i = 0 to 32 do
    remove (string_of_int i) numbers
  done;
  Alcotest.(check bool)
    "After a full removal, root should be empty" true (is_empty numbers);
  ()

let depth = [ collision_mem; depth_full_empty ]

let para_add_mem =
  let add_bunch map i =
    for j = 0 to 64 do
      add (Format.sprintf "(%d, %d)" i j) 0 map
    done;
    let all = ref true in
    for j = 0 to 64 do
      all := !all && mem (Format.sprintf "(%d, %d)" i j) map
    done;
    !all
  in
  Alcotest.test_case "adds & mems" `Quick @@ fun () ->
  let scores = create () in
  let domains =
    Array.init 8 (fun i -> Domain.spawn (fun () -> add_bunch scores i))
  in
  let all_doms = Array.for_all Domain.join domains in
  Alcotest.(check bool)
    "Parallel adds and mems should all find their respective values" true
    all_doms;
  ()

let para_add_then_mem =
  let add_bunch map i =
    for j = 0 to 64 do
      add (Format.sprintf "(%d, %d)" i j) 0 map
    done
  in
  Alcotest.test_case "adds then mem" `Quick @@ fun () ->
  let scores = create () in
  let domains =
    Array.init 8 (fun i -> Domain.spawn (fun () -> add_bunch scores i))
  in
  Array.iteri
    (fun i d ->
      Domain.join d;
      for j = 0 to 64 do
        let res = mem (Format.sprintf "(%d, %d)" i j) scores in
        Alcotest.(check bool)
          "Parallel adds should all be found afterward" true res
      done)
    domains;
  ()

let para_add_contention =
  Alcotest.test_case "adds with contention" `Quick @@ fun () ->
  let scores = create () in
  let domains =
    Array.init 8 (fun i ->
        Domain.spawn (fun () -> add "Celeste" (13 + i) scores))
  in
  Array.iter Domain.join domains;
  let found = find_opt "Celeste" scores in
  match found with
  | None -> Alcotest.fail "Added value should be found even with contention"
  | Some k ->
      Alcotest.(check bool)
        "Added value should be a really possible value" true
        (k >= 13 && k <= 20);
      ()

let parallel = [ para_add_mem; para_add_then_mem; para_add_contention ]

let basic_snap =
  Alcotest.test_case "snapshot" `Quick @@ fun () ->
  let map = create () in
  add "Hello" "World" map;
  let map' = snapshot map in
  add "Not in" "snap" map;
  Alcotest.(check (option string))
    "Snapshot should not eat additions" (Some "World") (find_opt "Hello" map');
  Alcotest.(check (option string))
    "Snapshot should not eat additions" None (find_opt "Not in" map');
  ()

let snap_then_ops =
  Alcotest.test_case "Snapshot then operations" `Quick @@ fun () ->
  let map = create () in
  for i = 0 to 32 do
    add (string_of_int i) i map
  done;
  let map' = snapshot map in
  for i = 0 to 32 do
    let stri = string_of_int i in
    remove stri map;
    let found = find_opt stri map' in
    Alcotest.(check (option int))
      "Added value should still be present in snapshotted version" (Some i)
      found
  done

let snapshots = [ basic_snap; snap_then_ops ]

let save_single =
  Alcotest.test_case "save singleton trie" `Quick @@ fun () ->
  let scores = create () in
  add "Subnautica" 3 scores;
  save_as_dot string_of_int scores "singleton.dot";
  let gotten_ic = open_in "singleton.dot" in
  let expect_ic = open_in "../../../tests/singleton_expected.dot" in
  let eof = ref true in
  while !eof do
    try
      let gotten_line = input_line gotten_ic in
      let expect_line = input_line expect_ic in
      Alcotest.(check string)
        "Lines should match expected file" expect_line gotten_line
    with End_of_file -> eof := false
  done;
  close_in gotten_ic;
  close_in expect_ic

let save_12 =
  Alcotest.test_case "save size 12 trie" `Quick @@ fun () ->
  let scores = create () in
  add "Warband" 3 scores;
  add "Mount & Blade" 4 scores;
  add "Skyrim" 11 scores;
  add "Devil May Cry" 0 scores;
  add "Rocket League" 6 scores;
  add "Luigi's Mansion" 14 scores;
  add "Unreal Tournament" 8 scores;
  add "Xenoblade" 16 scores;
  add "Mario Kart DS" 17 scores;
  add "Hollow Knight" 20 scores;
  add "Cult of the Lamb" 800 scores;
  add "Hellblade" 20 scores;
  save_as_dot string_of_int scores "size12.dot";
  let gotten_ic = open_in "size12.dot" in
  let expect_ic = open_in "../../../tests/size12_expected.dot" in
  let eof = ref true in
  while !eof do
    try
      let gotten_line = input_line gotten_ic in
      let expect_line = input_line expect_ic in
      Alcotest.(check string)
        "Lines should match expected file" expect_line gotten_line
    with End_of_file -> eof := false
  done;
  close_in gotten_ic;
  close_in expect_ic

let savings = [ save_single; save_12 ]

let () =
  Alcotest.run "Everything"
    [
      ("Basic operations", basics);
      ("Sequential collisions", depth);
      ("Hand-made parallel cases", parallel);
      ("Snapshot interactions", snapshots);
      ("Saving to .dot files", savings);
    ]
