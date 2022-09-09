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
end

module List = struct
  include List

  (** Remove the first element such that pred elem is true, returning the new list and a boolean marking if the element was deleted or not *)
  let rec remove_map pred = function
    | [] -> ([], false)
    | x :: xs ->
        if pred x then (xs, true)
        else
          let filtered, res = remove_map pred xs in
          (x :: filtered, res)

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

(** Ugly but there's nothing in the stdlib... *)
let hex_to_binary s =
  let convert = function
    | '0' -> "0000"
    | '1' -> "0001"
    | '2' -> "0010"
    | '3' -> "0011"
    | '4' -> "0100"
    | '5' -> "0101"
    | '6' -> "0110"
    | '7' -> "0111"
    | '8' -> "1000"
    | '9' -> "1001"
    | 'a' | 'A' -> "1010"
    | 'b' | 'B' -> "1011"
    | 'c' | 'C' -> "1100"
    | 'd' | 'D' -> "1101"
    | 'e' | 'E' -> "1110"
    | 'f' | 'F' -> "1111"
    | c ->
        raise
        @@ Invalid_argument ("Invalid hexadecimal character: " ^ Char.escaped c)
  in
  String.fold_left (fun bin c -> bin ^ convert c) "" s
