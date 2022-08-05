let nb = try int_of_string Sys.argv.(1) with _ -> 100_000

module P = Paradict.Make (struct
  type t = int

  let compare = Int.compare
  let to_string = string_of_int
end)

let bench name metric f =
  let clock = Mtime_clock.counter () in
  let result = f () in
  let elapsed = Mtime.Span.to_ms (Mtime_clock.count clock) in
  Format.printf
    {|{"results": [{"name": %S, "metrics": [{"name": %S, "value": %f, "units": "ms"}]}]}@.|}
    name metric elapsed;
  result

let t =
  bench "single" "add" @@ fun () ->
  let t = P.create () in
  for i = 1 to nb do
    P.add i i t
  done;
  assert (P.size t = nb);
  t

let () =
  bench "single" "mem" @@ fun () ->
  for i = 1 to nb do
    assert (P.mem i t)
  done

let () =
  bench "single" "find_opt: Some" @@ fun () ->
  for i = 1 to nb do
    match P.find_opt i t with Some j -> assert (i = j) | None -> assert false
  done

let () =
  bench "single" "find_opt: None" @@ fun () ->
  for i = nb + 1 to 2 * nb do
    match P.find_opt i t with Some _ -> assert false | None -> ()
  done

let () =
  bench "single" "remove: inexistant" @@ fun () ->
  for i = nb + 1 to 2 * nb do
    assert (not (P.remove i t))
  done;
  assert (P.size t = nb)

let () =
  bench "single" "remove: all" @@ fun () ->
  for i = 1 to nb do
    assert (P.remove i t)
  done;
  assert (P.size t = 0);
  assert (P.is_empty t)
