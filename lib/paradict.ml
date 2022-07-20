open Extensions

module type Hashable = sig
  type t

  val to_string : t -> string
end

module type T = sig
  type key
  type 'a t

  val create : unit -> 'a t
  val mem : key -> 'a t -> bool
  val add : key -> 'a -> 'a t -> unit
  val find_opt : key -> 'a t -> 'a option
  val remove : key -> 'a t -> bool
  val print : 'a t -> unit
  val depth : 'a t -> int
end

module Make (H : Hashable) = struct
  type key = H.t

  type 'a t = { root : 'a iNode }
  and 'a iNode = { main : 'a mainNode Atomic.t }

  and 'a mainNode =
    | CNode of 'a cNode
    | TNode of 'a tNode
    | LNode of 'a leaf list

  and 'a cNode = { bmp : Int32.t; array : 'a branch array }
  and 'a tNode = { leaf : 'a leaf }
  and 'a branch = INode of 'a iNode | Leaf of 'a leaf
  and 'a leaf = { key : key; value : 'a }

  exception Recur

  let value_opt key leaf = if key = leaf.key then Some leaf.value else None

  let create () =
    { root = { main = Atomic.make @@ CNode { bmp = 0l; array = [||] } } }

  (** NOT ATOMIC. FOR DEBUGGING PURPOSES ONLY *)
  let print t =
    let print_cnode fmt cnode =
      Format.fprintf fmt "{CNode: %s, array length %d}\n"
        (Int32.to_string cnode.bmp)
        (Array.length cnode.array)
    in
    let print_tnode fmt _tnode = Format.fprintf fmt "tomb node here??" in
    let print_lnode fmt _lnode = Format.fprintf fmt "list node here??" in
    let printer fmt t =
      match Atomic.get t.root.main with
      | CNode x -> print_cnode fmt x
      | TNode x -> print_tnode fmt x
      | LNode x -> print_lnode fmt x
    in
    Format.fprintf Format.std_formatter "%a" printer t

  (** The depth of a tree is the number of INodes *)
  let depth t =
    let rec aux i =
      match Atomic.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun acc b ->
              match b with Leaf _ -> acc | INode i -> max acc (1 + aux i))
            1 cnode.array
      | TNode _ -> 1
      | LNode _ -> 1
    in
    aux t.root

  (** The maximum value for the `lvl` variable.
      This makes the maximum real depth to be 52 (unreachable in practice). *)
  let max_lvl = 256

  (* We only use 5 bits of the hashcode, depending on the level in the tree.
   * Note that `lvl` is always a multiple of 5. (5 = log2 32) *)
  let to_flag key lvl =
    let open Digestif.SHA256 in
    let hash_str =
      key |> H.to_string |> digest_string |> to_hex |> hex_to_binary
    in
    let relevant = String.sub hash_str (String.length hash_str - lvl - 5) 5 in
    let to_shift = int_of_string ("0b" ^ relevant) in
    Int32.shift_left 1l to_shift

  (** `bit` is a single bit flag (never 0)
   *  `pos` is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap *)
  let flagpos key lvl bitmap =
    let flag = to_flag key lvl in
    let pos =
      Ocaml_intrinsics.Int32.count_set_bits
      @@ Int32.logand (Int32.pred flag) bitmap
    in
    (flag, pos)

  let resurrect tombed =
    match Atomic.get tombed.main with
    | TNode { leaf } -> Leaf leaf
    | _ -> INode tombed

  let contract cnode lvl =
    if lvl > 0 && Array.length cnode.array = 1 then
      match cnode.array.(0) with
      | Leaf leaf -> TNode { leaf }
      | _ -> CNode cnode
    else CNode cnode

  let compress cnode lvl =
    let array =
      Array.map
        (function Leaf l -> Leaf l | INode i -> resurrect i)
        cnode.array
    in
    contract { bmp = cnode.bmp; array } lvl

  let clean t lvl =
    match t with
    | None -> ()
    | Some t -> (
        match Atomic.get t.main with
        | CNode cnode ->
            let _ignored =
              (* TODO: check if it is really ignored in the paper? *)
              Atomic.compare_and_set t.main (CNode cnode) (compress cnode lvl)
            in
            ()
        | _ -> ())

  let rec find_opt key t =
    let rec aux i key lvl parent =
      match Atomic.get i.main with
      | CNode cnode -> (
          let flag, pos = flagpos key lvl cnode.bmp in
          if Int32.logand cnode.bmp flag = 0l then None
          else
            match cnode.array.(pos) with
            | Leaf leaf -> value_opt key leaf
            | INode inner -> aux inner key (lvl + 5) (Some i))
      | LNode list -> List.find_map (value_opt key) list
      | TNode _ ->
          clean parent (lvl - 5);
          raise Recur
    in
    try aux t.root key 0 None with Recur -> find_opt key t

  let mem key t = Option.is_some (find_opt key t)

  let rec cnode_of_pair l1 l2 lvl =
    let flag1 = to_flag l1.key lvl in
    let flag2 = to_flag l2.key lvl in
    let bmp = Int32.logor flag1 flag2 in
    match compare flag1 flag2 with
    | 0 ->
        if lvl > max_lvl then
          (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
          CNode
            {
              bmp;
              array = [| INode { main = Atomic.make @@ LNode [ l1; l2 ] } |];
            }
        else
          (* Collision on this level, we need to go deeper *)
          CNode
            {
              bmp;
              array =
                [|
                  INode { main = Atomic.make @@ cnode_of_pair l1 l2 (lvl + 5) };
                |];
            }
    | 1 -> CNode { bmp; array = [| Leaf l2; Leaf l1 |] }
    | _ -> CNode { bmp; array = [| Leaf l1; Leaf l2 |] }

  let inserted cnode flag pos l =
    let new_bitmap = Int32.logor cnode.bmp flag in
    let new_array = Array.insert cnode.array pos (Leaf l) in
    { bmp = new_bitmap; array = new_array }

  let updated cnode pos inode =
    let array = Array.copy cnode.array in
    array.(pos) <- inode;
    { cnode with array }

  let rec add key value t =
    let rec aux i key value lvl parent =
      match Atomic.get i.main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos key lvl cnode.bmp in
          if Int32.logand cnode.bmp flag = 0l then (
            (* no flag collision means it's a free insertion *)
            let new_cnode = inserted cnode flag pos { key; value } in
            if not @@ Atomic.compare_and_set i.main cn (CNode new_cnode) then
              raise Recur)
          else
            (* collision, we need to go a level deeper in the tree *)
            match cnode.array.(pos) with
            | INode inner -> aux inner key value (lvl + 5) (Some i)
            | Leaf l ->
                if l.key <> key then (
                  let new_inode =
                    INode
                      {
                        main =
                          Atomic.make @@ cnode_of_pair l { key; value } (lvl + 5);
                      }
                  in
                  let new_cnode = updated cnode pos new_inode in
                  if not @@ Atomic.compare_and_set i.main cn (CNode new_cnode)
                  then raise Recur)
                else
                  (* we need to update the new value *)
                  let new_cnode = updated cnode pos (Leaf { key; value }) in
                  if not @@ Atomic.compare_and_set i.main cn (CNode new_cnode)
                  then raise Recur)
      | TNode _ ->
          clean parent (lvl - 5);
          raise Recur
      | LNode list as ln ->
          let new_list = LNode ({ key; value } :: list) in
          if not @@ Atomic.compare_and_set i.main ln new_list then raise Recur
    in

    try aux t.root key value 0 None with Recur -> add key value t

  let rec clean_parent parent t key lvl =
    let main = Atomic.get t.main in
    let p_main = Atomic.get parent.main in
    match p_main with
    | CNode cnode -> (
        let flag, pos = flagpos key lvl cnode.bmp in
        if Int32.logand flag cnode.bmp <> 0l && cnode.array.(pos) = INode t then
          match main with
          | TNode _ ->
              let new_cnode = updated cnode pos (resurrect t) in
              if
                not
                @@ Atomic.compare_and_set parent.main (CNode cnode)
                     (contract new_cnode lvl)
              then clean_parent parent t key lvl
          | _ -> ())
    | _ -> ()

  let removed cnode pos flag =
    let bmp = Int32.logand cnode.bmp (Int32.lognot flag) in
    let array = Array.remove cnode.array pos in
    { bmp; array }

  let rec remove key t =
    let rec aux i key lvl parent =
      match Atomic.get i.main with
      | CNode cnode as cn ->
          let flag, pos = flagpos key lvl cnode.bmp in
          if Int32.logand cnode.bmp flag = 0l then false
          else
            let res =
              match cnode.array.(pos) with
              | INode inner -> aux inner key (lvl + 5) (Some i)
              | Leaf l ->
                  if l.key <> key then false
                  else
                    let new_cnode = removed cnode pos flag in
                    let contracted = contract new_cnode lvl in
                    Atomic.compare_and_set i.main cn contracted || raise Recur
            in
            (if res then
             match (Atomic.get i.main, parent) with
             | TNode _, Some parent -> clean_parent parent i key (lvl - 5)
             (* parent = None means i is the root, and the root cannot have a TNode child. *)
             | _ -> ());
            res
      | TNode _ ->
          clean parent (lvl - 5);
          raise Recur
      | LNode list as ln ->
          let new_list, changed =
            List.remove_map (fun leaf -> leaf.key = key) list
          in
          changed
          && (Atomic.compare_and_set i.main ln (LNode new_list) || raise Recur)
    in
    try aux t.root key 0 None with Recur -> remove key t
end