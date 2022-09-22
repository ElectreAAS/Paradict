open Extensions
include Paradict_intf

module Make (H : Hashtbl.HashedType) = struct
  module Types = struct
    type key = H.t
    type z = Z [@@warning "-37"]
    type _ s = S : 'n -> 'n s [@@warning "-37"]
    type (_, _) eq = Eq : ('a, 'a) eq
    type _ kind = SK : 'n kind -> 'n s kind | ZK : z kind

    (** Parent option.

        None means we're at the root. *)
    type (_, _) koption =
      | KSome : 'a -> ('a, 'n s) koption
      | KNone : ('a, z) koption

    (** Reverse from above.
        Internal return type from operations that might delete nodes.

        None can't be returned for the root iNode. *)
    type (_, _) rkoption =
      | RKSome : 'a -> ('a, 'n) rkoption
      | RKNone : ('a, 'n s) rkoption

    type 'a t = { root : ('a, z) iNode }

    and gen = < >
    (** The type of generations is an empty object.

        This is a classic OCaml trick to ensure safe (in)equality:
        With this setup
        [let x = object end;; let y = object end;; let z = x;;]
        [x = y] is false but [x = z] is true.
        This avoids integer overflow and discarded gen objects will be garbage collected. *)

    and ('a, 'n) iNode = {
      main : ('a, 'n) mainNode Kcas.ref;
      gen : gen Kcas.ref;
    }

    and (_, _) mainNode =
      | CNode : ('a, 'n) cNode -> ('a, 'n) mainNode
      | TNode : 'a leaf option -> ('a, 'n s) mainNode
      | LNode : 'a leaf list -> ('a, 'n s) mainNode

    and ('a, 'n) cNode = { bmp : Int32.t; array : ('a, 'n) branch array }

    and ('a, 'n) branch =
      | INode : ('a, 'n s) iNode -> ('a, 'n) branch
      | Leaf : 'a leaf -> ('a, 'n) branch

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

  let rec clear t =
    let startgen = Kcas.get t.root.gen in
    let empty_mnode = CNode { bmp = 0l; array = [||] } in
    if not @@ gen_dcss t.root (Kcas.get t.root.main) empty_mnode startgen then
      clear t

  let rec lvl_of_kind : type n. int -> n kind -> int =
   fun acc n -> match n with ZK -> acc | SK n -> lvl_of_kind (acc + 5) n

  (* We only use 5 bits of the hashcode, depending on the level in the tree.
     Note that [lvl] is always a multiple of 5. (5 = log2 32) *)
  let hash_to_flag : type n. n kind -> int -> int32 option =
   fun k hash ->
    let lvl = lvl_of_kind 0 k in
    if lvl > Sys.int_size then None
    else
      let mask = 0x1F in
      let shifted = hash lsr lvl in
      let relevant = shifted land mask in
      Some (Int32.shift_left 1l relevant)

  (** [flag] is a single bit flag (never 0)

      [pos] is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap *)
  let flagpos k bitmap hash =
    match hash_to_flag k hash with
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

  let contract : type n. ('a, n) cNode -> n kind -> ('a, n) mainNode =
   fun cnode k ->
    match k with
    | SK _ -> (
        match cnode.array.(0) with
        | Leaf l when Array.length cnode.array = 1 -> TNode (Some l)
        | _ -> CNode cnode)
    | _ -> CNode cnode

  let compress cnode k =
    let array =
      Array.filter_map
        (function
          | Leaf _ as l -> Some l
          | INode inner as i -> resurrect i (Kcas.get inner.main))
        cnode.array
    in
    contract { cnode with array } k

  let clean i k gen =
    match Kcas.get i.main with
    | CNode cnode as cn ->
        let _ignored =
          (* TODO: it is ignored in the paper, but investigate if that is really wise *)
          gen_dcss i cn (compress cnode k) gen
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
          type n1 n2.
          ('a, n1) iNode ->
          n1 kind ->
          (('a, n2) iNode * n2 kind, n1) koption ->
          'a =
       fun i k parent_opt ->
        match Kcas.get i.main with
        | CNode cnode as cn -> (
            let flag, pos = flagpos k cnode.bmp hash in
            if Int32.logand flag cnode.bmp = 0l then raise Not_found
            else
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    aux inner (SK k) (KSome (i, k))
                  else if regenerate i cn pos (Kcas.get inner.main) startgen
                  then aux i k parent_opt
                  else loop ()
              | Leaf leaf ->
                  if H.equal leaf.key key then leaf.value else raise Not_found)
        | LNode lst ->
            let leaf = List.find (fun l -> H.equal l.key key) lst in
            leaf.value
        | TNode _ -> (
            match parent_opt with
            | KSome (parent, k') ->
                clean parent k' startgen;
                loop ())
      in
      aux t.root ZK KNone
    in
    loop ()

  let find_opt key t = try Some (find key t) with Not_found -> None
  let mem key t = Option.is_some (find_opt key t)

  let rec branch_of_pair :
      type n.
      'a leaf * int -> 'a leaf * int -> n s kind -> gen -> ('a, n) branch =
   fun (l1, h1) (l2, h2) k gen ->
    let flag1 = hash_to_flag k h1 in
    let flag2 = hash_to_flag k h2 in
    let new_main_node =
      match (flag1, flag2) with
      | Some flag1, Some flag2 ->
          let bmp = Int32.logor flag1 flag2 in
          let array =
            match Int32.unsigned_compare flag1 flag2 with
            | 0 ->
                (* Collision on this level, we need to go deeper *)
                [| branch_of_pair (l1, h1) (l2, h2) (SK k) gen |]
            | 1 -> [| Leaf l2; Leaf l1 |]
            | _ -> [| Leaf l1; Leaf l2 |]
          in
          CNode { bmp; array }
      | _ ->
          (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
          LNode [ l1; l2 ]
    in
    INode { main = Kcas.ref new_main_node; gen = Kcas.ref gen }

  (** TODO: this might be cool to start asynchronously *)
  let rec clean_parent parent k i hash startgen =
    if Kcas.get i.gen <> startgen then ()
    else
      let main = Kcas.get i.main in
      let p_main = Kcas.get parent.main in
      match p_main with
      | CNode cnode as cn -> (
          let flag, pos = flagpos k cnode.bmp hash in
          if Int32.logand flag cnode.bmp <> 0l && cnode.array.(pos) = INode i
          then
            match main with
            | TNode _ | LNode ([] | [ _ ]) ->
                let new_cnode =
                  match resurrect (INode i) main with
                  | Some resurrected -> cnode_with_update cnode resurrected pos
                  | None -> cnode_with_delete cnode flag pos
                in
                if not @@ gen_dcss parent cn (contract new_cnode k) startgen
                then clean_parent parent k i hash startgen
            | _ -> ())
      | _ -> ()

  let rec update_list key f = function
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
          let updated, removed = update_list key f xs in
          (x :: updated, removed)

  let update key f t =
    let hash = H.hash key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      (* Boolean return value to signal a mapping was deleted. *)
      let rec aux :
          type n1 n2.
          ('a, n1) iNode ->
          n1 kind ->
          (('a, n2) iNode * n2 kind * (n1, n2 s) eq, n1) koption ->
          bool =
       fun i k parent_opt ->
        match Kcas.get i.main with
        | CNode cnode as cn ->
            let flag, pos = flagpos k cnode.bmp hash in
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
                      aux inner (SK k) (KSome (i, k, Eq))
                    else if regenerate i cn pos (Kcas.get inner.main) startgen
                    then aux i k parent_opt
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
                          let contracted = contract new_cnode k in
                          gen_dcss i cn contracted startgen || raise Exit
                    else
                      match f None with
                      | Some value ->
                          (* We create a new entry colliding with a leaf, so we create a new level. *)
                          let new_pair =
                            branch_of_pair
                              (l, H.hash l.key)
                              ({ key; value }, hash)
                              (SK k) startgen
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
               match (Kcas.get i.main, parent_opt) with
               | (LNode ([] | [ _ ]) | TNode _), KSome (parent, k', Eq) ->
                   clean_parent parent k' i hash startgen
               | _ -> ());
              leaf_removed
        | TNode _ -> (
            match parent_opt with
            | KSome (parent, k', Eq) ->
                clean parent k' startgen;
                loop ())
        | LNode lst as ln ->
            let new_list, changed = update_list key f lst in
            let mainnode, needs_cleaning =
              match new_list with
              | [] -> (TNode None, true)
              | [ l ] -> (TNode (Some l), true)
              | _ -> (LNode new_list, false)
            in
            if gen_dcss i ln mainnode startgen then (
              (if needs_cleaning then
               match parent_opt with
               | KSome (parent, k', Eq) ->
                   clean_parent parent k' i hash startgen);
              changed)
            else loop ()
      in
      try aux t.root ZK KNone with Exit -> loop ()
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
    match Kcas.get t.root.main with CNode cnode -> cnode.bmp = 0l

  let rec size t =
    let startgen = Kcas.get t.root.gen in
    let rec aux :
        type n1 n2.
        ('a, n1) iNode ->
        n1 kind ->
        (('a, n2) iNode * n2 kind, n1) koption ->
        int =
     fun i k parent_opt ->
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun acc b ->
              match b with
              | Leaf _ -> acc + 1
              | INode inner -> acc + aux inner (SK k) (KSome (i, k)))
            0 cnode.array
      | TNode _ -> (
          match parent_opt with
          | KSome (parent, k') ->
              clean parent k' startgen;
              size t)
      | LNode lst -> List.length lst
    in
    aux t.root ZK KNone

  let fmi_cnode :
      type n.
      ('a, n) cNode ->
      n kind ->
      (key -> 'a -> 'a option) ->
      (('a, n s) iNode ->
      n s kind ->
      ((('a, n) branch * _, n s) rkoption, n s) koption) ->
      (('a, n) mainNode, n) rkoption =
   fun cnode k f recur ->
    let rec aux :
        ('a, n) branch list -> int list -> int -> (('a, n) mainNode, n) rkoption
        =
     fun l_mapped l_filtered pos ->
      if pos < 0 then
        match (l_mapped, k) with
        | [], SK _ -> RKNone
        | [ Leaf l ], SK _ -> RKSome (TNode (Some l))
        | _ ->
            let array = Array.of_list l_mapped in
            let bmp = remove_from_bitmap cnode.bmp l_filtered in
            RKSome (CNode { bmp; array })
      else
        match cnode.array.(pos) with
        | INode inner -> (
            match recur inner (SK k) with
            | KSome (RKSome (branch, _)) ->
                aux (branch :: l_mapped) l_filtered (pos - 1)
            | KSome RKNone -> aux l_mapped (pos :: l_filtered) (pos - 1))
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
      let rec aux :
          type n1 n2.
          ('a, n1) iNode ->
          n1 kind ->
          (('a, n2) iNode * n2 kind * (n1, n2 s) eq, n1) koption ->
          ((('a, n2) branch * (n1, n2 s) eq, n1) rkoption, n1) koption =
       fun i k parent_opt ->
        match Kcas.get i.main with
        | CNode cnode as cn -> (
            let new_main_node =
              fmi_cnode cnode k f (fun inner k' ->
                  aux inner k' (KSome (i, k, Eq)))
            in
            match new_main_node with
            | RKNone ->
                if gen_dcss i cn (TNode None) startgen then KSome RKNone
                else raise Exit
            | RKSome mainnode ->
                if gen_dcss i cn mainnode startgen then
                  match parent_opt with
                  | KNone -> KNone
                  | KSome (_, _, Eq) -> KSome (RKSome (INode i, Eq))
                else raise Exit)
        | TNode _ -> (
            match parent_opt with
            | KSome (p, k, Eq) ->
                clean p k startgen;
                raise Exit)
        | LNode list as ln -> (
            let new_list =
              List.filter_map
                (fun { key; value } ->
                  match f key value with
                  | Some value -> Some { key; value }
                  | None -> None)
                list
            in
            match (new_list, parent_opt) with
            | [], _ -> KSome RKNone
            | [ l ], KSome (_, _, Eq) -> KSome (RKSome (Leaf l, Eq))
            | _, KSome (_, _, Eq) ->
                if gen_dcss i ln (LNode new_list) startgen then
                  KSome (RKSome (INode i, Eq))
                else raise Exit)
      in
      try ignore @@ aux t.root ZK KNone with Exit -> loop ()
    in
    loop ()

  let map f t =
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type n1 n2.
          ('a, n1) iNode ->
          n1 kind ->
          (('a, n2) iNode * n2 kind, n1) koption ->
          ('b, n1) iNode =
       fun i k parent_opt ->
        match Kcas.get i.main with
        | CNode cnode ->
            let array =
              Array.map
                (function
                  | INode inner -> INode (aux inner (SK k) (KSome (i, k)))
                  | Leaf { key; value } -> Leaf { key; value = f key value })
                cnode.array
            in
            {
              main = Kcas.ref (CNode { cnode with array });
              gen = Kcas.ref startgen;
            }
        | TNode _ -> (
            match parent_opt with
            | KSome (parent, k') ->
                clean parent k' startgen;
                raise Exit)
        | LNode list ->
            let new_list =
              List.map
                (function { key; value } -> { key; value = f key value })
                list
            in
            { main = Kcas.ref (LNode new_list); gen = Kcas.ref startgen }
      in
      try aux t.root ZK KNone with Exit -> loop ()
    in
    { root = loop () }

  let rec exists pred t =
    let startgen = Kcas.get t.root.gen in
    let rec aux :
        type n1 n2.
        ('a, n1) iNode ->
        n1 kind ->
        (('a, n2) iNode * n2 kind, n1) koption ->
        'b =
     fun i k parent ->
      match Kcas.get i.main with
      | CNode cnode ->
          Array.exists
            (function
              | Leaf { key; value } -> pred key value
              | INode inner -> aux inner (SK k) (KSome (i, k)))
            cnode.array
      | TNode _ -> (
          match parent with
          | KSome (parent, k') ->
              clean parent k' startgen;
              exists pred t)
      | LNode list -> List.exists (fun { key; value } -> pred key value) list
    in
    aux t.root ZK KNone

  let rec for_all pred t =
    let startgen = Kcas.get t.root.gen in
    let rec aux :
        type n1 n2.
        ('a, n1) iNode ->
        n1 kind ->
        (('a, n2) iNode * n2 kind, n1) koption ->
        'b =
     fun i k parent ->
      match Kcas.get i.main with
      | CNode cnode ->
          Array.for_all
            (function
              | Leaf { key; value } -> pred key value
              | INode inner -> aux inner (SK k) (KSome (i, k)))
            cnode.array
      | TNode _ -> (
          match parent with
          | KSome (parent, k') ->
              clean parent k' startgen;
              for_all pred t)
      | LNode list -> List.for_all (fun { key; value } -> pred key value) list
    in
    aux t.root ZK KNone

  let rec iter f t =
    let startgen = Kcas.get t.root.gen in
    let rec aux :
        type n1 n2.
        ('a, n1) iNode ->
        n1 kind ->
        (('a, n2) iNode * n2 kind, n1) koption ->
        'b =
     fun i k parent ->
      match Kcas.get i.main with
      | CNode cnode ->
          Array.iter
            (function
              | Leaf { key; value } -> f key value
              | INode inner -> aux inner (SK k) (KSome (i, k)))
            cnode.array
      | TNode _ -> (
          match parent with
          | KSome (parent, k') ->
              clean parent k' startgen;
              iter f t)
      | LNode list -> List.iter (fun { key; value } -> f key value) list
    in
    aux t.root ZK KNone

  let rec fold f t init =
    let startgen = Kcas.get t.root.gen in
    let rec aux :
        type n1 n2.
        'a ->
        ('b, n1) iNode ->
        n1 kind ->
        (('b, n2) iNode * n2 kind, n1) koption ->
        'a =
     fun acc i k parent_opt ->
      match Kcas.get i.main with
      | CNode cnode ->
          Array.fold_left
            (fun inner_acc branch ->
              match branch with
              | Leaf { key; value } -> f key value inner_acc
              | INode inner -> aux inner_acc inner (SK k) (KSome (i, k)))
            acc cnode.array
      | TNode _ -> (
          match parent_opt with
          | KSome (parent, k') ->
              clean parent k' startgen;
              fold f t init)
      | LNode list ->
          List.fold_left
            (fun inner_acc { key; value } -> f key value inner_acc)
            acc list
    in
    aux init t.root ZK KNone

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
    let rec pr_inode : type n. ('a, n) iNode -> unit =
     fun inode ->
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
    and pr_cnode : type n. ('a, n) cNode -> unit =
     fun cnode ->
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
