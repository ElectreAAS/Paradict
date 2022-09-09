let nb = try int_of_string Sys.argv.(1) with _ -> 100_000
let max_domains = try int_of_string Sys.argv.(2) with _ -> 8

module P = Paradict.Make (struct
  type t = int

  let equal = ( = )
  let hash t = Hashtbl.hash t
end)

let bench name metric f =
  let clock = Mtime_clock.counter () in
  let result = f () in
  let elapsed = Mtime.Span.to_ms (Mtime_clock.count clock) in
  Format.printf
    {|{"results": [{"name": %S, "metrics": [{"name": %S, "value": %f, "units": "ms"}]}]}@.|}
    name metric elapsed;
  result

module T = Domainslib.Task

module Test (Config : sig
  val name : string
  val pool : T.pool
end) =
struct
  let bench metric f = bench "multicore" (metric ^ "/" ^ Config.name) f
  let iter start finish body = T.parallel_for Config.pool ~start ~finish ~body

  let t =
    bench "add" @@ fun () ->
    let t = P.create () in
    iter 1 nb (fun i -> P.add i i t);
    assert (P.size t = nb);
    t

  let () = bench "mem" @@ fun () -> iter 1 nb (fun i -> assert (P.mem i t))

  let () =
    bench "find_opt: Some" @@ fun () ->
    iter 1 nb (fun i ->
        match P.find_opt i t with
        | Some j -> assert (i = j)
        | None -> assert false)

  let () =
    bench "find_opt: None" @@ fun () ->
    iter (nb + 1) (2 * nb) (fun i ->
        match P.find_opt i t with Some _ -> assert false | None -> ())

  let () =
    bench "remove: all" @@ fun () ->
    iter 1 nb (fun i -> P.remove i t);
    assert (P.size t = 0);
    assert (P.is_empty t)
end

let run domains =
  let module Config = struct
    let name = string_of_int domains
    let pool = T.setup_pool ~num_additional_domains:(domains - 1) ()
  end in
  T.run Config.pool (fun () ->
      let module Run = Test (Config) in
      ());
  T.teardown_pool Config.pool

let () =
  for domains = 1 to max_domains do
    Format.printf "@.Benchmark with %i domains:@." domains;
    run domains
  done
