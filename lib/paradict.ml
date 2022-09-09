open Extensions
include Paradict_intf

module Make (H : Hashtbl.HashedType) = struct
  module Types = struct
    type key = H.t
    type at_root
    type not_root

    type 'a t = { root : ('a, at_root) iNode }

    and gen = < >
    (** The type of generations is an empty object.

        This is a classic OCaml trick to ensure safe (in)equality:
        With this setup
        [let x = object end;; let y = object end;; let z = x;;]
        [x = y] is false but [x = z] is true.
        This avoids integer overflow and discarded gen objects will be garbage collected. *)

    and ('a, 'r) iNode = {
      main : ('a, 'r) mainNode Kcas.ref;
      gen : gen Kcas.ref;
    }

    and (_, _) mainNode =
      | CNode : 'a cNode -> ('a, 'r) mainNode
      | TNode : 'a leaf option -> ('a, not_root) mainNode
      | LNode : 'a leaf list -> ('a, not_root) mainNode

    and 'a cNode = { bmp : Int32.t; array : 'a branch array }

    and _ branch =
      | INode : ('a, 'r) iNode -> 'a branch
      | Leaf : 'a leaf -> 'a branch

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
          main = Kcas.ref (CNode { bmp = 0l; array = [||] });
          gen = Kcas.ref (object end);
        };
    }

  let create_complex k1 k2 =
    let gen = Kcas.ref (object end) in
    let t : (int, not_root) mainNode = TNode { key = k1; value = 0 } in
    let i1 : int branch = INode { main = Kcas.ref t; gen } in
    let c : (int, 'anything) mainNode =
      CNode { bmp = 1l; array = [| Leaf { key = k2; value = 8 } |] }
    in
    let i2 : int branch = INode { main = Kcas.ref c; gen } in
    let master_c : (int, at_root) mainNode =
      CNode { bmp = 3l; array = [| i1; i2 |] }
    in
    { root = { gen; main = Kcas.ref master_c } }

  let rec clear t =
    let startgen = Kcas.get t.root.gen in
    let empty_mnode = CNode { bmp = 0l; array = [||] } in
    if not @@ gen_dcss t.root (Kcas.get t.root.main) empty_mnode startgen then
      clear t

  (* We only use 5 bits of the hash, depending on the level in the tree.
     Note that [lvl] is always a multiple of 5. (5 = log2 32) *)
  let hash_to_flag lvl hash =
    if lvl > Sys.int_size then None
    else
      let mask = 0x1F in
      let shifted = hash lsr lvl in
      let relevant = shifted land mask in
      Some (Int32.shift_left 1l relevant)

  (** [flag] is a single bit flag (never 0)

      [pos] is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap *)
  let flagpos lvl bitmap hash =
    match hash_to_flag lvl hash with
    | Some flag ->
        let open Int32 in
        let pos = pred flag |> logand bitmap |> popcount in
        (flag, pos)
    | None -> failwith "Maximum depth reached but flagpos was still used???"

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
    let res = aux 0 bmp l_filtered 0 in
    res

  let resurrect i = function
    | TNode None | LNode [] -> None
    | TNode (Some l) | LNode [ l ] -> Some (Leaf l)
    | _ -> Some i

  let contract : 'a cNode -> int -> ('a, not_root) mainNode =
   fun cnode lvl ->
    if lvl > 0 then
      match Array.length cnode.array with
      | 0 -> TNode None
      | 1 -> (
          match cnode.array.(0) with
          | Leaf leaf -> TNode (Some leaf)
          | _ -> CNode cnode)
      | _ -> CNode cnode
    else CNode cnode

  let compress cnode lvl =
    let array =
      Array.filter_map
        (function
          | Leaf _ as l -> Some l
          | INode i as inner -> resurrect inner (Kcas.get i.main))
        cnode.array
    in
    contract { cnode with array } lvl

  let clean : type r. ('a, r) iNode -> int -> gen -> unit =
   fun parent lvl startgen ->
    (* match parent with
       | None ->
           (* no parent means it's the root, nothing to do as it cannot have a tnode child. *)
           ()
       | Some t -> *)
    let x = match r with not_root -> failwith "wait" | _ -> failwith "todo" in
    match Kcas.get parent.main with
    | CNode cnode as cn ->
        let _ignored =
          (* TODO: it is ignored in the paper, but investigate if that is really wise *)
          gen_dcss parent cn (compress cnode lvl) startgen
        in
        ()
    | _ -> ()

  let cnode_with_insert cnode leaf flag pos =
    let new_bitmap = Int32.logor cnode.bmp flag in
    let new_array = Array.insert cnode.array pos (Leaf leaf) in
    { bmp = new_bitmap; array = new_array }

  let cnode_with_update cnode branch pos =
    let array = Array.copy cnode.array in
    array.(pos) <- branch;
    { cnode with array }

  let cnode_with_delete cnode flag pos =
    let bmp = Int32.logxor cnode.bmp flag in
    let array = Array.remove cnode.array pos in
    { bmp; array }

  (** Update the generation of the immediate child [cn.array.(pos)] of [parent] to [new_gen].
      We volontarily do not update the generations of deeper INodes, as this is a lazy algorithm.
      TODO: investigate if this is really wise. *)
  let regenerate parent cn pos child_main new_gen =
    match cn with
    | CNode cnode ->
        let new_cnode =
          cnode_with_update cnode
            (INode { main = Kcas.ref child_main; gen = Kcas.ref new_gen })
            pos
        in
        gen_dcss parent cn (CNode new_cnode) new_gen
    | _ -> failwith "Cannot happen, solve with GADT (1)"

  let find key t =
    let hash = H.hash key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type r1 r2. ('a, r1) iNode -> int -> ('a, r2) iNode option -> 'a =
       fun i lvl parent ->
        match Kcas.get i.main with
        | CNode cnode as cn -> (
            let flag, pos = flagpos lvl cnode.bmp hash in
            if Int32.logand flag cnode.bmp = 0l then raise Not_found
            else
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    aux inner (lvl + 5)
                      (Some (* FIXME: this doesn't compile *) i)
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
    let new_main_node =
      match (flag1, flag2) with
      | Some flag1, Some flag2 ->
          let bmp = Int32.logor flag1 flag2 in
          let array =
            match Int32.unsigned_compare flag1 flag2 with
            | 0 ->
                (* Collision on this level, we need to go deeper *)
                [| branch_of_pair (l1, h1) (l2, h2) (lvl + 5) gen |]
            | 1 -> [| Leaf l2; Leaf l1 |]
            | _ -> [| Leaf l1; Leaf l2 |]
          in
          CNode { bmp; array }
      | _ ->
          (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
          LNode [ l1; l2 ]
    in
    INode { main = Kcas.ref new_main_node; gen = Kcas.ref gen }

  let rec clean_parent parent i hash lvl startgen =
    if Kcas.get i.gen <> startgen then ()
    else
      let main = Kcas.get i.main in
      let p_main = Kcas.get parent.main in
      match p_main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos lvl cnode.bmp hash in
          if Int32.logand flag cnode.bmp <> 0l && cnode.array.(pos) = INode i
          then
            match main with
            | TNode _ | LNode ([] | [ _ ]) ->
                let new_cnode =
                  match resurrect (INode i) main with
                  | Some resurrected -> cnode_with_update cnode resurrected pos
                  | None -> cnode_with_delete cnode flag pos
                in
                if not @@ gen_dcss parent cn (contract new_cnode lvl) startgen
                then clean_parent parent i hash lvl startgen
            | _ -> ())
      | _ -> ()

  let update key f t =
    let hash = H.hash key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      (* Boolean return value to signal a mapping was deleted. *)
      let rec aux i lvl parent : bool =
        match Kcas.get i.main with
        | CNode cnode as cn ->
            let flag, pos = flagpos lvl cnode.bmp hash in
            if Int32.logand flag cnode.bmp = 0l then
              (* No flag collision, the key isn't in the map. *)
              match f None with
              | Some value ->
                  (* We need to insert it. *)
                  let new_cnode =
                    cnode_with_insert cnode { key; value } flag pos
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
                    else raise Exit
                | Leaf l -> (
                    if H.equal l.key key then
                      match f (Some l.value) with
                      | Some value ->
                          (* We found a value to be updated. *)
                          let new_cnode =
                            cnode_with_update cnode (Leaf { key; value }) pos
                          in
                          if gen_dcss i cn (CNode new_cnode) startgen then false
                          else raise Exit
                      | None ->
                          (* We need to remove this value *)
                          let new_cnode = cnode_with_delete cnode flag pos in
                          let contracted = contract new_cnode lvl in
                          gen_dcss i cn contracted startgen || raise Exit
                    else
                      match f None with
                      | Some value ->
                          (* We create a new entry colliding with a leaf, so we create a new level. *)
                          let new_pair =
                            branch_of_pair
                              (l, H.hash l.key)
                              ({ key; value }, hash)
                              (lvl + 5) startgen
                          in
                          let new_cnode =
                            cnode_with_update cnode new_pair pos
                          in
                          if gen_dcss i cn (CNode new_cnode) startgen then false
                          else raise Exit
                      (* The key isn't in the map. *)
                      | None -> false)
              in
              (if leaf_removed then
               match (Kcas.get i.main, parent) with
               | (LNode ([] | [ _ ]) | TNode _), Some parent ->
                   clean_parent parent i hash (lvl - 5) startgen
               (* 'parent = None' means i is the root, and the root can only have a cnode child. *)
               | _ -> ());
              leaf_removed
        | TNode _ ->
            clean parent (lvl - 5) startgen;
            loop ()
        | LNode lst as ln ->
            let rec update_list = function
              | [] -> (
                  match f None with
                  | None -> ([], false)
                  | Some v -> ([ { key; value = v } ], false))
              | x :: xs ->
                  if H.equal x.key key then
                    match f (Some x.value) with
                    | Some v -> ({ key; value = v } :: xs, false)
                    | None -> (xs, true)
                  else
                    let updated, removed = update_list xs in
                    (x :: updated, removed)
            in
            let new_list, changed = update_list lst in
            let mainnode, needs_cleaning =
              match new_list with
              | [] -> (TNode None, true)
              | [ l ] -> (TNode (Some l), true)
              | _ -> (LNode new_list, false)
            in
            if gen_dcss i ln mainnode startgen then (
              (if needs_cleaning then
               match parent with
               | Some parent -> clean_parent parent i hash (lvl - 5) startgen
               | None -> failwith "Cannot happen, solve with GADT (2)");
              changed)
            else loop ()
      in
      try aux t.root 0 None with Exit -> loop ()
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
    match Kcas.get t.root.main with
    | CNode cnode -> cnode.bmp = 0l
    | _ -> failwith "Cannot happen, solve with GADT (3)"

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

  let fmi_cnode cnode f lvl recur =
    let rec aux l_mapped l_filtered pos =
      if pos < 0 then
        match l_mapped with
        | [] when lvl > 0 -> None
        | [ Leaf l ] when lvl > 0 -> Some (TNode (Some l))
        | _ ->
            let array = Array.of_list l_mapped in
            let bmp = remove_from_bitmap cnode.bmp l_filtered in
            Some (CNode { bmp; array })
      else
        match cnode.array.(pos) with
        | INode inner -> (
            match recur inner (lvl + 5) with
            | Some branch -> aux (branch :: l_mapped) l_filtered (pos - 1)
            | None -> aux l_mapped (pos :: l_filtered) (pos - 1))
        | Leaf { key; value } -> (
            match f key value with
            | Some value ->
                aux (Leaf { key; value } :: l_mapped) l_filtered (pos - 1)
            | None -> aux l_mapped (pos :: l_filtered) (pos - 1))
    in
    aux [] [] (Array.length cnode.array - 1)

  let filter_map_inplace f t =
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux i lvl parent =
        match Kcas.get i.main with
        | CNode cnode as cn -> (
            let new_main_node =
              fmi_cnode cnode f lvl (fun inner lvl -> aux inner lvl (Some i))
            in
            match new_main_node with
            | None ->
                if gen_dcss i cn (TNode None) startgen then None else loop ()
            | Some mainnode ->
                if gen_dcss i cn mainnode startgen then Some (INode i)
                else loop ())
        | TNode _ ->
            clean parent (lvl - 5) startgen;
            loop ()
        | LNode list as ln -> (
            let new_list =
              List.filter_map
                (fun { key; value } ->
                  match f key value with
                  | Some value -> Some { key; value }
                  | None -> None)
                list
            in
            match new_list with
            | [] -> None
            | [ l ] -> Some (Leaf l)
            | _ ->
                if gen_dcss i ln (LNode new_list) startgen then Some (INode i)
                else loop ())
      in
      aux t.root 0 None
    in
    ignore (loop ())

  let map f t =
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux i lvl parent =
        match Kcas.get i.main with
        | CNode cnode ->
            let array =
              Array.map
                (function
                  | INode inner -> INode (aux inner (lvl + 5) (Some i))
                  | Leaf { key; value } -> Leaf { key; value = f key value })
                cnode.array
            in
            {
              main = Kcas.ref (CNode { cnode with array });
              gen = Kcas.ref startgen;
            }
        | TNode _ ->
            clean parent (lvl - 5) startgen;
            loop ()
        | LNode list ->
            let new_list =
              List.map
                (function { key; value } -> { key; value = f key value })
                list
            in
            { main = Kcas.ref (LNode new_list); gen = Kcas.ref startgen }
      in
      aux t.root 0 None
    in
    { root = loop () }

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

  let rec fold f t init =
    let startgen = Kcas.get t.root.gen in
    let rec aux acc i lvl parent =
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun inner_acc branch ->
              match branch with
              | Leaf { key; value } -> f key value inner_acc
              | INode inner -> aux inner_acc inner (lvl + 5) (Some i))
            acc cnode.array
      | TNode _ ->
          clean parent (lvl - 5) startgen;
          fold f t init
      | LNode list ->
          List.fold_left
            (fun inner_acc { key; value } -> f key value inner_acc)
            acc list
    in
    aux init t.root 0 None

  let save_as_dot (string_of_key, string_of_val) t filename =
    let oc = open_out filename in
    let ic (* cnode *) = ref 0 in
    let il (* lnode *) = ref 0 in
    let ii (* inode *) = ref 0 in
    let it (* tnode *) = ref 0 in
    let iv (* leaf (value) *) = ref 0 in
    Printf.fprintf oc
      "digraph {\n\troot [shape=plaintext];\n\troot -> I0 [style=dotted];\n";
    let pr_cnode_info cnode =
      Printf.fprintf oc "\tC%d [shape=record label=\"<bmp> 0x%lX" !ic cnode.bmp;
      Array.iteri (fun i _ -> Printf.fprintf oc "|<i%d> ·" i) cnode.array;
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
      Printf.fprintf oc
        "\tT%d [style=filled shape=box fontcolor=white color=black];\n" !it;
      (match leaf with
      | Some leaf ->
          pr_leaf_info leaf;
          Printf.fprintf oc "\tT%d -> V%d;\n" !it !iv;
          iv := !iv + 1
      | None -> ());
      it := !it + 1
    and pr_list list =
      Printf.fprintf oc "\tL%d [style=filled fontcolor=white color=red];\n" !il;
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
