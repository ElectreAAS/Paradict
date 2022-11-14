let nb = try int_of_string Sys.argv.(1) with _ -> 100_000
let max_domains = try int_of_string Sys.argv.(2) with _ -> 8

module Int = struct
  type t = int

  let equal = Int.equal
  let hash t = Hashtbl.hash t
end

module StrMap = Map.Make (struct
  type t = string

  let compare = compare
end)

module IntMap = Map.Make (struct
  type t = int

  let compare = compare
end)

module type S = sig
  type key
  type 'a t

  val create : unit -> 'a t
  val add : key -> 'a -> 'a t -> unit
  val remove : key -> 'a t -> unit
  val find_opt : key -> 'a t -> 'a option
  val update : key -> ('a option -> 'a option) -> 'a t -> unit
  val filter_map_inplace : (key -> 'a -> 'a option) -> 'a t -> unit
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

  let filter_map_inplace f t =
    with_lock t @@ fun () -> H.filter_map_inplace f t.tbl

  let iter fn t = with_lock t @@ fun () -> H.iter fn t.tbl
  let fold fn t z = with_lock t @@ fun () -> H.fold fn t.tbl z
end

let bench name domains is_left map f =
  let clock = Mtime_clock.counter () in
  let result = f () in
  let elapsed = Mtime.Span.to_ms (Mtime_clock.count clock) in
  let set = StrMap.find name !map in
  let new_set =
    IntMap.update domains
      (fun prev ->
        let other = Option.value prev ~default:(0.0, 0.0) in
        Some (if is_left then (elapsed, snd other) else (fst other, elapsed)))
      set
  in
  map := StrMap.add name new_set !map;
  result

module T = Domainslib.Task

module Test
    (P : S with type key = int) (Config : sig
      val is_left : bool
      val domains : int
      val pool : T.pool
      val map : (float * float) IntMap.t StrMap.t ref
    end) =
struct
  let bench name f = bench name Config.domains Config.is_left Config.map f
  let iter start finish body = T.parallel_for Config.pool ~start ~finish ~body

  let t =
    bench "add" @@ fun () ->
    let t = P.create () in
    iter 1 nb (fun i -> P.add i i t);
    t

  let () =
    bench "find_opt" @@ fun () ->
    iter (nb / 2) (3 * nb / 2) (fun i -> ignore @@ P.find_opt i t)

  let () =
    bench "update" @@ fun () ->
    iter 1 (2 * nb) (fun i ->
        P.update i (fun _ -> if i mod 2 = 0 then Some i else None) t)

  let () =
    bench "filter_map_inplace" @@ fun () ->
    P.filter_map_inplace (fun i _ -> Some (i / 2)) t

  let () =
    bench "iter" @@ fun () ->
    let sum = ref 0 in
    P.iter (fun k _ -> sum := !sum + k) t

  let () =
    bench "fold" @@ fun () -> ignore @@ P.fold (fun k _ acc -> acc + k) t 0

  let () = bench "remove" @@ fun () -> iter 1 nb (fun i -> P.remove (2 * i) t)
end

let run is_left (module P : S with type key = int) domains map =
  let module Config = struct
    let is_left = is_left
    let domains = domains
    let pool = T.setup_pool ~num_domains:(domains - 1) ()
    let map = map
  end in
  T.run Config.pool (fun () ->
      let module Run = Test (P) (Config) in
      ());
  T.teardown_pool Config.pool

let () =
  let operations =
    [
      "add";
      "find_opt";
      "update";
      "filter_map_inplace";
      "iter";
      "fold";
      "remove";
    ]
  in
  let results =
    ref
    @@ List.fold_left
         (fun m op -> StrMap.add op IntMap.empty m)
         StrMap.empty operations
  in
  for domains = 1 to max_domains do
    run true (module P) domains results;
    run false (module Naive) domains results
  done;
  let print name =
    let set = StrMap.find name !results in
    for i = 1 to max_domains do
      let p, h = IntMap.find i set in
      Format.printf "%d %f %f@." i p h
    done;
    Format.printf "@.@."
  in
  List.iter print operations;
  Format.printf
    "{\"foo\": `@Arthur Wendling <!Arthur Wendling> Sanitize your inputs!}\n"
