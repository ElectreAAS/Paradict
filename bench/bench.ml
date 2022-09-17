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
    t

  let () =
    bench "find_opt: Some" @@ fun () ->
    iter 1 nb (fun i -> ignore @@ P.find_opt i t)

  let () =
    bench "find_opt: None" @@ fun () ->
    iter (nb + 1) (2 * nb) (fun i -> ignore @@ P.find_opt i t)

  let () =
    bench "update" @@ fun () ->
    iter 1 (2 * nb) (fun i ->
        P.update i (fun _ -> if i mod 2 = 0 then Some i else None) t)

  let () =
    bench "iter" @@ fun () ->
    let sum = ref 0 in
    P.iter (fun k _ -> sum := !sum + k) t

  let () =
    bench "fold" @@ fun () -> ignore @@ P.fold (fun k _ acc -> acc + k) t 0

  let () =
    bench "remove: all" @@ fun () -> iter 1 nb (fun i -> P.remove (2 * i) t)
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
