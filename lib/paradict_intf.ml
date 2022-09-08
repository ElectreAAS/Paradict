module type Hashable = sig
  type t

  val compare : t -> t -> int
  val to_string : t -> string
end

module type S = sig
  type key
  type 'a t

  val create : unit -> 'a t
  (** Create an empty dictionnary. *)

  val clear : 'a t -> unit
  (** Empty the dictionnary. *)

  val find : key -> 'a t -> 'a
  (** Return the current mapping of the key, or raises {!Not_found}. *)

  val find_opt : key -> 'a t -> 'a option
  (** Return [Some v] if [v] is mapped with the key, or [None]. *)

  val mem : key -> 'a t -> bool
  (** Return [true] if the key is present in the map, or [false]. *)

  val update : key -> ('a option -> 'a option) -> 'a t -> unit
  (** [update k f m] changes the mapping of [k] to [f (find_opt k m)].

    If [f (Some v)] returns [None], the mapping [v] is removed. *)

  val add : key -> 'a -> 'a t -> unit
  (** Add the (key, value) pair to the map, overwriting any previous mapping. *)

  val remove : key -> 'a t -> unit
  (** Remove the mapping of the key *)

  val snapshot : 'a t -> 'a t
  (** Take a snapshot of the map. This is a O(1) operation.

      The original map can still be used, even in concurrent contexts. *)

  val is_empty : 'a t -> bool
  (** Return [true] if the map contains no data.
      Result might not be accurate in concurrent contexts. Take a snapshot first! *)

  val size : 'a t -> int
  (** Return the number of elements in the map.
      Result might not be accurate in concurrent contexts. Take a snapshot first! *)

  val iter : (key -> 'a -> unit) -> 'a t -> unit
  (** Apply a function to all mappings in unspecified order.
      The behavior is not specified if the map is modified (by the function
        or concurrent access) during the iteration. *)

  val map : (key -> 'a -> 'a) -> ?high_contention_strat:bool -> 'a t -> unit
  (** [map f t] replaces every value [v] associated with key [k] by the result of calling [f k v], in unspecified order.

      The optional parameter [high_contention_strat] (set to [false] by default)
      may improve performances in the case of high contention and lengthy [f] calls.
      If [f] is side-effecting, those effects may happen an unspecified number of times, as operations are restarted in case of CAS conflicts.
      Use a pure function instead. *)

  val fold : (key -> 'a -> 'b -> 'b) -> 'a t -> 'b -> 'b
  (** [fold f t init] computes [f k1 v1 ... (f kN vN init)] where [k1...kN] are the keys
      and [v1...vN] the associated values, in unspecified order.

      If [f] is side-effecting, those effects may happen an unspecified number of times, as operations are restarted in case of CAS conflicts.
      Use a pure function instead. *)

  val cast : (key -> 'a -> 'b) -> 'a t -> 'b t
  (** Cast is the more traditional function called map or fmap.
      The original map is untouched.
      TODO: find a better name & review if it is really needed. *)

  val exists : (key -> 'a -> bool) -> 'a t -> bool
  (** Check that there exists at least one (key, value) pair satisfying the predicate. *)

  val for_all : (key -> 'a -> bool) -> 'a t -> bool
  (** Check that all (key, value) pairs satisfy the predicate. *)

  val save_as_dot : ('a -> string) -> 'a t -> string -> unit
  (** Save the map as a graph in a .dot file.

      Mainly for debugging purposes. *)
end

module type Make = functor (H : Hashable) -> S with type key = H.t

module type Intf = sig
  module type Hashable = Hashable
  module type S = S

  module Make : Make
end
