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

  val find : key -> 'a t -> 'a
  (** Return the current mapping of the key, or raises {!Not_found}. *)

  val find_opt : key -> 'a t -> 'a option
  (** Return [Some v] if [v] is mapped with the key, or [None]. *)

  val mem : key -> 'a t -> bool
  (** Return [true] if the key is present in the map, or [false]. *)

  val update : key -> ('a option -> 'a option) -> 'a t -> unit
  (** [update k f m] changes the mapping of [k] to [f (find_opt k m)].
    If [f] returns [None], the mapping is removed. *)

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
