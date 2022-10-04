module Array = struct
  include Array

  (** Inserts an element at position index within the array, shifting all elements after it to the right.
      @raise Invalid_argument
      if index is outside the range 0 to length a. *)
  let insert a index elem =
    if index < 0 || index > length a then
      raise @@ Invalid_argument "Array.insert";
    init
      (length a + 1)
      (fun i ->
        match compare i index with 0 -> elem | 1 -> a.(i - 1) | _ -> a.(i))

  (** Remove an element at position index within the array, shifting all elements after it to the left.
      @raise Invalid_argument
      if index is outside the range 0 to length a or if a is empty *)
  let remove a index =
    if index < 0 || index >= length a || length a = 0 then
      raise @@ Invalid_argument "Array.remove";
    init (length a - 1) (fun i -> if i < index then a.(i) else a.(i + 1))

  (** [filter_map f a] applies [f] to all elements in [a], filtering [None] results,
      and returning the array of [Some] results, along with a sorted list of the indices of deleted elements. *)
  let filter_map f a =
    let rec aux i l_mapped l_filtered =
      if i < 0 then (of_list l_mapped, l_filtered)
      else
        match f a.(i) with
        | Some x -> aux (i - 1) (x :: l_mapped) l_filtered
        | None -> aux (i - 1) l_mapped (i :: l_filtered)
    in
    aux (length a - 1) [] []
end

module List = struct
  include List

  let print pp l =
    let fmt = Format.std_formatter in
    let rec aux = function
      | [ x ] -> Format.fprintf fmt "%s]\n%!" (pp x)
      | x :: xs ->
          Format.fprintf fmt "%s; " (pp x);
          aux xs
      | [] -> Format.fprintf fmt "]\n%!"
    in
    Format.fprintf fmt "[";
    aux l
end

module Int32 = struct
  include Int32

  let popcount n =
    let rec aux n count =
      if n = 0l then count
      else
        let shifted = shift_right_logical n 1 in
        if logand 1l n <> 0l then aux shifted (count + 1) else aux shifted count
    in
    aux n 0
end
