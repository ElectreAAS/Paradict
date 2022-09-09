open Extensions
include Paradict_intf

module Make (H : HashedType) = struct
  module Types = struct
    type key = H.t

    type 'a t = { root : 'a iNode }

    and gen = < >
    (** The type of generations is an empty object.

        This is a classic OCaml trick to ensure safe (in)equality:
        With this setup
        [let x = object end;; let y = object end;; let z = x;;]
        [x = y] is false but [x = z] is true.
        This avoids integer overflow and discarded gen objects will be garbage collected. *)

    and 'a iNode = { main : 'a mainNode Kcas.ref; gen : gen Kcas.ref }

    and 'a mainNode =
      | CNode of 'a cNode
      | TNode of 'a leaf
      | LNode of 'a leaf list

    and 'a cNode = { bmp : Int32.t; array : 'a branch array }
    and 'a branch = INode of 'a iNode | Leaf of 'a leaf
    and 'a leaf = { key : key; value : 'a }
  end

  include Types

  (** Generational Double Compare Single Swap *)
  let gen_dcss inode old_m new_m gen =
    let cas = Kcas.mk_cas inode.main old_m new_m in
    let atomic_read = Kcas.mk_cas inode.gen gen gen in
    Kcas.kCAS [ cas; atomic_read ]

  let create () =
    {
      root =
        {
          main = Kcas.ref @@ CNode { bmp = 0l; array = [||] };
          gen = Kcas.ref (object end);
        };
    }

  let rec clear t =
    let startgen = Kcas.get t.root.gen in
    let empty_mnode = CNode { bmp = 0l; array = [||] } in
    if not @@ gen_dcss t.root (Kcas.get t.root.main) empty_mnode startgen then
      clear t

  (** This is the only function that actually hashes keys.
      We try to use it as infrequently as possible. *)
  let hash_to_binary key = key |> H.hash |> Printf.sprintf "%x" |> hex_to_binary

  (* We only use 5 bits of the hashcode, depending on the level in the tree.
     Note that [lvl] is always a multiple of 5. (5 = log2 32) *)
  let hash_to_flag lvl hashcode =
    try
      let relevant = String.sub hashcode lvl 5 in
      let to_shift = int_of_string ("0b" ^ relevant) in
      Int32.shift_left 1l to_shift
    with Invalid_argument _ ->
      (* Invalid argument means the lvl is too high for String.sub. *)
      Int32.zero

  (** [flag] is a single bit flag (never 0)

      [pos] is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap *)
  let flagpos lvl bitmap hashcode =
    let flag = hash_to_flag lvl hashcode in
    let pos =
      Ocaml_intrinsics.Int32.count_set_bits
      @@ Int32.logand bitmap (Int32.pred flag)
    in
    (flag, pos)

  let resurrect branch =
    match branch with
    | Leaf _ -> branch
    | INode i -> (
        match Kcas.get i.main with TNode leaf -> Leaf leaf | _ -> branch)

  let contract cnode lvl =
    if lvl > 0 && Array.length cnode.array = 1 then
      match cnode.array.(0) with Leaf leaf -> TNode leaf | _ -> CNode cnode
    else CNode cnode

  let compress cnode lvl =
    let array = Array.map resurrect cnode.array in
    contract { cnode with array } lvl

  let clean parent lvl startgen =
    match parent with
    | None ->
        (* no parent means it's the root, nothing to do as it cannot have a tnode child. *)
        ()
    | Some t -> (
        match Kcas.get t.main with
        | CNode cnode as cn ->
            let _ignored =
              (* TODO: it is ignored in the paper, but investigate if that is really wise *)
              gen_dcss t cn (compress cnode lvl) startgen
            in
            ()
        | _ -> ())

  let cnode_with_insert cnode flag pos leaf =
    let new_bitmap = Int32.logor cnode.bmp flag in
    let new_array = Array.insert cnode.array pos (Leaf leaf) in
    { bmp = new_bitmap; array = new_array }

  let cnode_with_update cnode pos branch =
    let array = Array.copy cnode.array in
    array.(pos) <- branch;
    { cnode with array }

  (** Update the generation of the immediate child [cn.array.(pos)] of [parent] to [new_gen].
      We volontarily do not update the generations of deeper INodes, as this is a lazy algorithm.
      TODO: investigate if this is really wise. *)
  let regenerate parent cn pos child_main new_gen =
    match cn with
    | CNode cnode ->
        let new_cnode =
          cnode_with_update cnode pos
            (INode { main = Kcas.ref child_main; gen = Kcas.ref new_gen })
        in
        gen_dcss parent cn (CNode new_cnode) new_gen
    | _ -> true

  let find key t =
    let hashcode = hash_to_binary key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux i lvl parent =
        match Kcas.get i.main with
        | CNode cnode as cn -> (
            let flag, pos = flagpos lvl cnode.bmp hashcode in
            if Int32.logand flag cnode.bmp = 0l then raise Not_found
            else
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    aux inner (lvl + 5) (Some i)
                  else if regenerate i cn pos (Kcas.get inner.main) startgen
                  then aux i lvl parent
                  else loop ()
              | Leaf leaf ->
                  if H.equal leaf.key key then leaf.value else raise Not_found)
        | LNode lst ->
            let leaf = List.find (fun l -> H.equal l.key key) lst in
            leaf.value
        | TNode _ ->
            clean parent (lvl - 5) startgen;
            loop ()
      in
      aux t.root 0 None
    in
    loop ()

  let find_opt key t = try Some (find key t) with Not_found -> None
  let mem key t = Option.is_some (find_opt key t)

  let rec branch_of_pair (l1, h1) (l2, h2) lvl gen =
    let flag1 = hash_to_flag lvl h1 in
    let flag2 = hash_to_flag lvl h2 in
    let bmp = Int32.logor flag1 flag2 in
    let new_main_node =
      if bmp = 0l then
        (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
        LNode [ l1; l2 ]
      else
        let array =
          match Int32.unsigned_compare flag1 flag2 with
          | 0 ->
              (* Collision on this level, we need to go deeper *)
              [| branch_of_pair (l1, h1) (l2, h2) (lvl + 5) gen |]
          | 1 -> [| Leaf l2; Leaf l1 |]
          | _ -> [| Leaf l1; Leaf l2 |]
        in
        CNode { bmp; array }
    in
    INode { main = Kcas.ref @@ new_main_node; gen = Kcas.ref gen }

  let rec clean_parent parent i hashcode lvl startgen =
    if Kcas.get i.gen <> startgen then ()
    else
      let main = Kcas.get i.main in
      let p_main = Kcas.get parent.main in
      match p_main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos lvl cnode.bmp hashcode in
          if Int32.logand flag cnode.bmp <> 0l && cnode.array.(pos) = INode i
          then
            match main with
            | TNode _ ->
                let new_cnode =
                  cnode_with_update cnode pos (resurrect (INode i))
                in
                if not @@ gen_dcss parent cn (contract new_cnode lvl) startgen
                then clean_parent parent i hashcode lvl startgen
            | _ -> ())
      | _ -> ()

  let cnode_with_delete cnode pos flag =
    let bmp = Int32.logand cnode.bmp (Int32.lognot flag) in
    let array = Array.remove cnode.array pos in
    { bmp; array }

  let update key f t =
    let hashcode = hash_to_binary key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      (* Boolean return value to signal a mapping was deleted. *)
      let rec aux i lvl parent : bool =
        match Kcas.get i.main with
        | CNode cnode as cn ->
            let flag, pos = flagpos lvl cnode.bmp hashcode in
            if Int32.logand flag cnode.bmp = 0l then
              (* No flag collision, the key isn't in the map. *)
              match f None with
              | Some value ->
                  (* We need to insert it. *)
                  let new_cnode =
                    cnode_with_insert cnode flag pos { key; value }
                  in
                  if gen_dcss i cn (CNode new_cnode) startgen then false
                  else loop ()
              | None -> false
            else
              let leaf_removed =
                match cnode.array.(pos) with
                | INode inner ->
                    if Kcas.get inner.gen = startgen then
                      aux inner (lvl + 5) (Some i)
                    else if regenerate i cn pos (Kcas.get inner.main) startgen
                    then aux i lvl parent
                    else loop ()
                | Leaf l -> (
                    if H.equal l.key key then
                      match f (Some l.value) with
                      | Some value ->
                          (* We found a value to be updated. *)
                          let new_cnode =
                            cnode_with_update cnode pos (Leaf { key; value })
                          in
                          if gen_dcss i cn (CNode new_cnode) startgen then false
                          else loop ()
                      | None ->
                          (* We need to remove this value *)
                          let new_cnode = cnode_with_delete cnode pos flag in
                          let contracted = contract new_cnode lvl in
                          gen_dcss i cn contracted startgen || loop ()
                    else
                      match f None with
                      | Some value ->
                          (* We create a new entry colliding with a leaf, so we create a new level. *)
                          let new_pair =
                            branch_of_pair
                              (l, hash_to_binary l.key)
                              ({ key; value }, hashcode)
                              (lvl + 5) startgen
                          in
                          let new_cnode =
                            cnode_with_update cnode pos new_pair
                          in
                          if gen_dcss i cn (CNode new_cnode) startgen then false
                          else loop ()
                      (* The key isn't in the map. *)
                      | None -> false)
              in
              (if leaf_removed then
               match (Kcas.get i.main, parent) with
               | TNode _, Some parent ->
                   clean_parent parent i hashcode (lvl - 5) startgen
               (* 'parent = None' means i is the root, and the root cannot have a TNode child. *)
               | _ -> ());
              leaf_removed
        | TNode _ ->
            clean parent (lvl - 5) startgen;
            loop ()
        | LNode lst as ln ->
            let new_list, changed =
              List.remove_map (fun leaf -> H.equal leaf.key key) lst
            in
            changed && (gen_dcss i ln (LNode new_list) startgen || loop ())
      in
      aux t.root 0 None
    in
    ignore (loop ())

  let add key value t = update key (fun _ -> Some value) t
  let remove key t = update key (fun _ -> None) t

  let rec snapshot t =
    let main = Kcas.get t.root.main in
    let atomic_read = Kcas.mk_cas t.root.main main main in
    (* The old root is updated to a new generation *)
    let cas = Kcas.mk_cas t.root.gen (Kcas.get t.root.gen) (object end) in
    if Kcas.kCAS [ cas; atomic_read ] then
      (* We can return a new root with a second new generation *)
      (* TODO: investigate why 2 new generations *)
      { root = { main = Kcas.ref main; gen = Kcas.ref (object end) } }
    else snapshot t

  let copy = snapshot

  let is_empty t =
    match Kcas.get t.root.main with CNode cnode -> cnode.bmp = 0l | _ -> false

  let rec size t =
    let startgen = Kcas.get t.root.gen in
    let rec aux i lvl parent =
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun acc b ->
              match b with
              | Leaf _ -> acc + 1
              | INode inner -> acc + aux inner (lvl + 5) (Some i))
            0 cnode.array
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          size t
      | LNode lst -> List.length lst
    in
    aux t.root 0 None

  (* This function assumes l_filtered is sorted and it only contains valid indexes *)
  let remove_from_bitmap bmp l_filtered =
    let rec aux cursor bmp l_filtered seen =
      if cursor >= 32 then bmp
      else
        let bit = Int32.shift_left 1l cursor in
        let flag = Int32.logand bit bmp in
        if flag <> 0l then
          match l_filtered with
          | x :: xs when x = seen ->
              aux (cursor + 1)
                (Int32.logand bmp (Int32.lognot flag))
                xs (seen + 1)
          | _ -> aux (cursor + 1) bmp l_filtered (seen + 1)
        else aux (cursor + 1) bmp l_filtered seen
    in
    let res =
      aux
        (Ocaml_intrinsics.Int32.count_trailing_zeros_nonzero_arg bmp)
        bmp l_filtered 0
    in
    res

  let rec filter_map_inplace f t =
    let startgen = Kcas.get t.root.gen in
    let rec aux i lvl parent =
      match Kcas.get i.main with
      | CNode cnode as cn -> (
          let rec filter_cnode l_mapped l_filtered pos =
            if pos < 0 then
              match l_mapped with
              | [] ->
                  (* The entirety of the cnode contents was filtered *)
                  None
              | [ Leaf l ] when lvl > 0 -> Some (TNode l)
              | _ ->
                  let array = Array.of_list l_mapped in
                  let bmp = remove_from_bitmap cnode.bmp l_filtered in
                  Some (CNode { bmp; array })
            else
              match cnode.array.(pos) with
              | Leaf { key; value } -> (
                  match f key value with
                  | Some value ->
                      filter_cnode
                        (Leaf { key; value } :: l_mapped)
                        l_filtered (pos - 1)
                  | None -> filter_cnode l_mapped (pos :: l_filtered) (pos - 1))
              | INode inner -> (
                  match aux inner (lvl + 5) (Some i) with
                  | Some branch ->
                      filter_cnode (branch :: l_mapped) l_filtered (pos - 1)
                  | None -> filter_cnode l_mapped (pos :: l_filtered) (pos - 1))
          in
          let new_main_node =
            filter_cnode [] [] (Array.length cnode.array - 1)
          in
          match new_main_node with
          | Some (TNode l) -> Some (Leaf l)
          | Some mainnode ->
              if gen_dcss i cn mainnode startgen then Some (INode i)
              else raise Exit
          | None -> None)
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          raise Exit
      | LNode list as ln ->
          let new_list =
            List.filter_map
              (fun { key; value } ->
                match f key value with
                | Some value -> Some { key; value }
                | None -> None)
              list
          in
          if gen_dcss i ln (LNode new_list) startgen then Some (INode i)
          else raise Exit
    in
    try ignore @@ aux t.root 0 None with Exit -> filter_map_inplace f t

  let map f t =
    let startgen = Kcas.get t.root.gen in
    let rec aux i =
      match Kcas.get i.main with
      | CNode cnode ->
          let array =
            Array.map
              (function
                | INode inner -> INode (aux inner)
                | Leaf { key; value } -> Leaf { key; value = f key value })
              cnode.array
          in
          {
            main = Kcas.ref @@ CNode { cnode with array };
            gen = Kcas.ref startgen;
          }
      | TNode _ -> failwith "TODO: finish this if necessary"
      | LNode list ->
          let new_list =
            List.map
              (function { key; value } -> { key; value = f key value })
              list
          in
          { main = Kcas.ref @@ LNode new_list; gen = Kcas.ref startgen }
    in
    { root = aux t.root }

  let rec reduce (array_fn, list_fn) f t =
    let startgen = Kcas.get t.root.gen in
    let rec aux i lvl parent =
      match Kcas.get i.main with
      | CNode cnode ->
          array_fn
            (function
              | Leaf { key; value } -> f key value
              | INode inner -> aux inner (lvl + 5) (Some i))
            cnode.array
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          reduce (array_fn, list_fn) f t
      | LNode list -> list_fn (fun { key; value } -> f key value) list
    in
    aux t.root 0 None

  let exists pred = reduce (Array.exists, List.exists) pred
  let for_all pred = reduce (Array.for_all, List.for_all) pred
  let iter f = reduce (Array.iter, List.iter) f

  let rec fold (f : key -> 'a -> 'b -> 'b) (t : 'a t) (init : 'b) : 'b =
    let startgen = Kcas.get t.root.gen in
    let rec aux acc i lvl parent =
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun inner_acc brnch ->
              match brnch with
              | Leaf { key; value } -> f key value inner_acc
              | INode inner -> aux inner_acc inner (lvl + 5) (Some i))
            acc cnode.array
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          fold f t init
      | LNode list ->
          List.fold_left (fun acc { key; value } -> f key value acc) init list
    in
    aux init t.root 0 None

  let save_as_dot (string_of_key, string_of_val) t filename =
    let oc = open_out filename in
    let ic = ref 0 in
    let il = ref 0 in
    let ii = ref 0 in
    let it = ref 0 in
    let iv = ref 0 in
    Printf.fprintf oc
      "digraph {\n\troot [shape=plaintext];\n\troot -> I0 [style=dotted];\n";
    let pr_cnode_info cnode =
      let size = Int32.unsigned_to_int cnode.bmp in
      let bmp = match size with Some n -> string_of_int n | None -> "..." in
      Printf.fprintf oc "\tC%d [shape=record label=\"<bmp> %s" !ic bmp;
      for i = 0 to Ocaml_intrinsics.Int32.count_set_bits cnode.bmp - 1 do
        Printf.fprintf oc "|<i%d> ·" i
      done;
      Printf.fprintf oc "\"];\n"
    in
    let pr_leaf_info leaf =
      Printf.fprintf oc
        "\tV%d [shape=Mrecord label=\"<key> %s|<val> %s\" style=filled \
         color=gold];\n"
        !iv (string_of_key leaf.key) (string_of_val leaf.value)
    in
    let rec pr_inode inode =
      let self = !ii in
      ii := !ii + 1;
      Printf.fprintf oc "\tI%d [style=filled shape=box color=green2];\n" self;
      match Kcas.get inode.main with
      | CNode cnode ->
          pr_cnode_info cnode;
          Printf.fprintf oc "\tI%d -> C%d:bmp;\n" self !ic;
          pr_cnode cnode
      | TNode leaf ->
          Printf.fprintf oc "\tI%d -> T%d;\n" self !it;
          pr_tnode leaf
      | LNode list ->
          Printf.fprintf oc "\tI%d -> L%d;\n" self !il;
          pr_list list
    and pr_cnode cnode =
      let self = !ic in
      ic := self + 1;
      Array.iteri
        (fun i b ->
          match b with
          | INode inner ->
              Printf.fprintf oc "\tC%d:i%d -> I%d;\n" self i !ii;
              pr_inode inner
          | Leaf leaf ->
              pr_leaf_info leaf;
              Printf.fprintf oc "\tC%d:i%d -> V%d;\n" self i !iv;
              iv := !iv + 1)
        cnode.array
    and pr_tnode leaf =
      pr_leaf_info leaf;
      Printf.fprintf oc
        "\tT%d [style=filled shape=box fontcolor=white color=black];\n" !it;
      Printf.fprintf oc "\tT%d -> V%d;\n" !it !iv;
      iv := !iv + 1;
      it := !it + 1
    and pr_list list =
      List.iter
        (fun l ->
          pr_leaf_info l;
          Printf.fprintf oc "\tL%d -> V%d [color=red style=bold];\n" !il !iv;
          iv := !iv + 1)
        list;
      il := !il + 1
    in
    pr_inode t.root;
    Printf.fprintf oc "}\n%!";
    close_out oc
end
