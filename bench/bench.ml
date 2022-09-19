let nb = try int_of_string Sys.argv.(1) with _ -> 100_000
let max_domains = try int_of_string Sys.argv.(2) with _ -> 8

module Int = struct
  type t = int

  let equal = Int.equal
  let hash t = Hashtbl.hash t
end

module type S = sig
  type key
  type 'a t

  val create : unit -> 'a t
  val add : key -> 'a -> 'a t -> unit
  val remove : key -> 'a t -> unit
  val find_opt : key -> 'a t -> 'a option
  val update : key -> ('a option -> 'a option) -> 'a t -> unit
  val iter : (key -> 'a -> unit) -> 'a t -> unit
  val fold : (key -> 'a -> 'b -> 'b) -> 'a t -> 'b -> 'b
end

module P = Paradict.Make (Int)

module Naive = struct
  module H = Hashtbl.Make (Int)

  type key = H.key
  type 'a t = { lock : Mutex.t; tbl : 'a H.t }

  let create () = { lock = Mutex.create (); tbl = H.create 16 }

  let with_lock t fn =
    Mutex.lock t.lock;
    let result = fn () in
    Mutex.unlock t.lock;
    result

  let add k v t = with_lock t @@ fun () -> H.add t.tbl k v
  let remove k t = with_lock t @@ fun () -> H.remove t.tbl k
  let find_opt k t = with_lock t @@ fun () -> H.find_opt t.tbl k

  let update k fn t =
    with_lock t @@ fun () ->
    let opt = H.find_opt t.tbl k in
    match (opt, fn opt) with
    | None, None -> ()
    | Some v, Some v' when v == v' -> ()
    | None, Some v -> H.add t.tbl k v
    | Some _, Some v -> H.replace t.tbl k v
    | Some _, None -> H.remove t.tbl k

  let iter fn t = with_lock t @@ fun () -> H.iter fn t.tbl
  let fold fn t z = with_lock t @@ fun () -> H.fold fn t.tbl z
end

let bench name metric f =
  let clock = Mtime_clock.counter () in
  let result = f () in
  let elapsed = Mtime.Span.to_ms (Mtime_clock.count clock) in
  Format.printf
    {|{"results": [{"name": %S, "metrics": [{"name": %S, "value": %f, "units": "ms"}]}]}@.|}
    name metric elapsed;
  result

module T = Domainslib.Task

module Test
    (P : S with type key = int) (Config : sig
      val target : string
      val name : string
      val pool : T.pool
    end) =
struct
  let bench metric f = bench Config.target (metric ^ "/" ^ Config.name) f
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

let run ~target (module P : S with type key = int) domains =
  let module Config = struct
    let target = target
    let name = string_of_int domains
    let pool = T.setup_pool ~num_additional_domains:(domains - 1) ()
  end in
  T.run Config.pool (fun () ->
      let module Run = Test (P) (Config) in
      ());
  T.teardown_pool Config.pool

let () =
  for domains = 1 to max_domains do
    Format.printf "@.## Benchmark with %i domains:@." domains;
    run ~target:"Paradict" (module P) domains;
    Format.printf "@.";
    run ~target:"Hashtbl" (module Naive) domains
  done
