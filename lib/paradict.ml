open Extensions
include Paradict_intf

module Make (H : Hashable) = struct
  module Types = struct
    type key = H.t

    type 'a t = { root : 'a iNode }
    and gen = < >
    and 'a iNode = { main : 'a mainNode Kcas.ref; gen : gen Kcas.ref }

    and 'a mainNode =
      | CNode of 'a cNode
      | TNode of 'a leaf
      | LNode of 'a leaf list

    and 'a cNode = { bmp : Int32.t; array : 'a branch array }
    and 'a branch = INode of 'a iNode | Leaf of 'a leaf
    and 'a leaf = { key : key; value : 'a }

    exception Recur
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

  (** Only correct in sequential contexts. *)
  let is_empty t =
    match Kcas.get t.root.main with CNode cnode -> cnode.bmp = 0l | _ -> false

  (** Print a given tree to a filename, in .dot format.
    * Use `dot -Tsvg <filename> >output.svg` to see it!
    *)
  let save_as_dot string_of_val t filename =
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
        !iv (H.to_string leaf.key) (string_of_val leaf.value)
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
      Printf.fprintf oc "\tT%d [shape=box style=box color=black];\n" !it;
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

  (** The depth of a tree is the number of INodes.
      It is only correct in sequential contexts. *)
  let depth t =
    let rec aux i =
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun acc b ->
              match b with Leaf _ -> acc | INode i -> max acc (1 + aux i))
            1 cnode.array
      | _ -> 1
    in
    aux t.root

  (** Only correct in sequential contexts. *)
  let size t =
    let rec aux i =
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun acc b ->
              match b with Leaf _ -> acc + 1 | INode i -> acc + aux i)
            0 cnode.array
      | TNode _ -> 1
      | LNode lst -> List.length lst
    in
    aux t.root

  (** The maximum value for the `lvl` variable.
      This makes the maximum real depth to be 52 (unreachable in practice). *)
  let max_lvl = 256

  let hash_to_binary key =
    let open Digestif.SHA256 in
    key |> H.to_string |> digest_string |> to_hex |> hex_to_binary

  (* We only use 5 bits of the hashcode, depending on the level in the tree.
   * Note that `lvl` is always a multiple of 5. (5 = log2 32) *)
  let hash_to_flag lvl hashcode =
    let relevant = String.sub hashcode (String.length hashcode - lvl - 5) 5 in
    let to_shift = int_of_string ("0b" ^ relevant) in
    Int32.shift_left 1l to_shift

  (** `flag` is a single bit flag (never 0)
   *  `pos` is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap *)
  let flagpos lvl bitmap hashcode =
    let flag = hash_to_flag lvl hashcode in
    let pos =
      Ocaml_intrinsics.Int32.count_set_bits
      @@ Int32.logand (Int32.pred flag) bitmap
    in
    (flag, pos)

  let resurrect tombed =
    match Kcas.get tombed.main with
    | TNode leaf -> Leaf leaf
    | _ -> INode tombed

  let contract cnode lvl =
    if lvl > 0 && Array.length cnode.array = 1 then
      match cnode.array.(0) with Leaf leaf -> TNode leaf | _ -> CNode cnode
    else CNode cnode

  let compress cnode lvl =
    let array =
      Array.map
        (function Leaf l -> Leaf l | INode i -> resurrect i)
        cnode.array
    in
    contract { bmp = cnode.bmp; array } lvl

  let clean t lvl startgen =
    match t with
    | None -> ()
    | Some t -> (
        match Kcas.get t.main with
        | CNode cnode as cn ->
            let _ignored =
              (* TODO: it is ignored in the paper, but investigate if that is really wise *)
              gen_dcss t cn (compress cnode lvl) startgen
            in
            ()
        | _ -> ())

  let inserted cnode flag pos leaf =
    let new_bitmap = Int32.logor cnode.bmp flag in
    let new_array = Array.insert cnode.array pos (Leaf leaf) in
    { bmp = new_bitmap; array = new_array }

  let updated cnode pos inode =
    let array = Array.copy cnode.array in
    array.(pos) <- inode;
    { cnode with array }

  (** Update the generation of the immediate child cnode.array.(pos) of parent to new_gen.
      We volontarily do not update the generations of deeper INodes, as this is a lazy algorithm.
      TODO: investigate if this is really wise. *)
  let regenerate parent cn pos child_main new_gen =
    match cn with
    | CNode cnode ->
        let new_cnode =
          updated cnode pos
            (INode { main = Kcas.ref child_main; gen = Kcas.ref new_gen })
        in
        gen_dcss parent cn (CNode new_cnode) new_gen
    | _ -> true

  let find key t =
    let hashcode = hash_to_binary key in
    let rec aux i lvl parent startgen =
      match Kcas.get i.main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos lvl cnode.bmp hashcode in
          if Int32.logand cnode.bmp flag = 0l then raise Not_found
          else
            match cnode.array.(pos) with
            | INode inner ->
                if Kcas.get inner.gen = startgen then
                  aux inner (lvl + 5) (Some i) startgen
                else if regenerate i cn pos (Kcas.get inner.main) startgen then
                  aux i lvl parent startgen
                else raise Recur
            | Leaf leaf ->
                if H.compare leaf.key key = 0 then leaf.value
                else raise Not_found)
      | LNode lst ->
          let leaf = List.find (fun l -> H.compare l.key key = 0) lst in
          leaf.value
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          raise Recur
    in
    let rec loop () =
      try aux t.root 0 None (Kcas.get t.root.gen) with Recur -> loop ()
    in
    loop ()

  let find_opt key t = try Some (find key t) with Not_found -> None
  let mem key t = Option.is_some (find_opt key t)

  let rec branch_of_pair (l1, h1) (l2, h2) lvl gen =
    let flag1 = hash_to_flag lvl h2 in
    let flag2 = hash_to_flag lvl h2 in
    let bmp = Int32.logor flag1 flag2 in
    match Int32.unsigned_compare flag1 flag2 with
    | 0 ->
        if lvl > max_lvl then
          (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
          INode { main = Kcas.ref @@ LNode [ l1; l2 ]; gen = Kcas.ref gen }
        else
          (* Collision on this level, we need to go deeper *)
          INode
            {
              main =
                Kcas.ref
                @@ CNode
                     {
                       bmp;
                       array =
                         [| branch_of_pair (l1, h1) (l2, h2) (lvl + 5) gen |];
                     };
              gen = Kcas.ref gen;
            }
    | 1 ->
        INode
          {
            main = Kcas.ref @@ CNode { bmp; array = [| Leaf l2; Leaf l1 |] };
            gen = Kcas.ref gen;
          }
    | _ ->
        INode
          {
            main = Kcas.ref @@ CNode { bmp; array = [| Leaf l1; Leaf l2 |] };
            gen = Kcas.ref gen;
          }

  let add key value t =
    let hashcode = hash_to_binary key in
    let rec aux i lvl parent startgen =
      match Kcas.get i.main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos lvl cnode.bmp hashcode in
          if Int32.logand cnode.bmp flag = 0l then (
            (* no flag collision means it's a free insertion *)
            let new_cnode = inserted cnode flag pos { key; value } in
            if not @@ gen_dcss i cn (CNode new_cnode) startgen then raise Recur)
          else
            (* collision, we need to go a level deeper in the tree *)
            match cnode.array.(pos) with
            | INode inner ->
                if Kcas.get inner.gen = startgen then
                  aux inner (lvl + 5) (Some i) startgen
                else if regenerate i cn pos (Kcas.get inner.main) startgen then
                  aux i lvl parent startgen
                else raise Recur
            | Leaf l ->
                if H.compare l.key key = 0 then (
                  (* No need to go deeper, just to update the new value *)
                  let new_cnode = updated cnode pos (Leaf { key; value }) in
                  if not @@ gen_dcss i cn (CNode new_cnode) startgen then
                    raise Recur)
                else
                  let new_pair =
                    branch_of_pair
                      (l, hash_to_binary l.key)
                      ({ key; value }, hashcode)
                      (lvl + 5) startgen
                  in
                  let new_cnode = updated cnode pos new_pair in
                  if not @@ gen_dcss i cn (CNode new_cnode) startgen then
                    raise Recur)
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          raise Recur
      | LNode lst as ln ->
          let new_list = LNode ({ key; value } :: lst) in
          if not @@ gen_dcss i ln new_list startgen then raise Recur
    in
    let rec loop () =
      try aux t.root 0 None (Kcas.get t.root.gen) with Recur -> loop ()
    in
    loop ()

  let rec clean_parent parent t hashcode lvl startgen =
    if Kcas.get t.gen <> startgen then ()
    else
      let main = Kcas.get t.main in
      let p_main = Kcas.get parent.main in
      match p_main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos lvl cnode.bmp hashcode in
          if Int32.logand flag cnode.bmp <> 0l && cnode.array.(pos) = INode t
          then
            match main with
            | TNode _ ->
                let new_cnode = updated cnode pos (resurrect t) in
                if not @@ gen_dcss parent cn (contract new_cnode lvl) startgen
                then clean_parent parent t hashcode lvl startgen
            | _ -> ())
      | _ -> ()

  let removed cnode pos flag =
    let bmp = Int32.logand cnode.bmp (Int32.lognot flag) in
    let array = Array.remove cnode.array pos in
    { bmp; array }

  let remove key t =
    let hashcode = hash_to_binary key in
    let rec aux i lvl parent startgen =
      match Kcas.get i.main with
      | CNode cnode as cn ->
          let flag, pos = flagpos lvl cnode.bmp hashcode in
          if Int32.logand cnode.bmp flag = 0l then false
          else
            let res =
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    aux inner (lvl + 5) (Some i) startgen
                  else if regenerate i cn pos (Kcas.get inner.main) startgen
                  then aux i lvl parent startgen
                  else raise Recur
              | Leaf l ->
                  if H.compare l.key key <> 0 then false
                  else
                    let new_cnode = removed cnode pos flag in
                    let contracted = contract new_cnode lvl in
                    gen_dcss i cn contracted startgen || raise Recur
            in
            (if res then
             match (Kcas.get i.main, parent) with
             | TNode _, Some parent ->
                 clean_parent parent i hashcode (lvl - 5) startgen
             (* 'parent = None' means i is the root, and the root cannot have a TNode child. *)
             | _ -> ());
            res
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          raise Recur
      | LNode lst as ln ->
          let new_list, changed =
            List.remove_map (fun leaf -> H.compare leaf.key key = 0) lst
          in
          changed && (gen_dcss i ln (LNode new_list) startgen || raise Recur)
    in
    let rec loop () =
      try aux t.root 0 None (Kcas.get t.root.gen) with Recur -> loop ()
    in
    loop ()

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
end
