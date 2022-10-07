open Extensions
include Paradict_intf

module Make (H : Hashtbl.HashedType) = struct
  module Types = struct
    type key = H.t
    type z = Zero [@@warning "-37"]
    type nz = NonZero [@@warning "-37"]
    type _ kind = Z : z kind | NZ : nz kind

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
      | TNode : 'a leaf option -> ('a, nz) mainNode
      | LNode : 'a leaf list -> ('a, nz) mainNode

    and ('a, 'n) cNode = { bmp : int; array : ('a, 'n) branch array }

    and (_, _) branch =
      | INode : ('a, nz) iNode -> ('a, 'n) branch
      | Leaf : 'a leaf -> ('a, 'n) branch

    and 'a leaf = { key : key; value : 'a }

    (** Inner return type of functions manipulating tries.
        - Type {!'a} is returned if all goes well.
        - Type {!'b} is returned when a cleaning needs to happen.
        - Type {!'n} is the height of the trie, to ensure that [Clean*] can't bubble to the root.
        - Type {!'c} marks the possibility of change, to ensure that {!CleanAfterDive} can't happen at all in read-only operations. *)
    type (_, _, _, _) status =
      | Alright : 'a -> ('a, 'b, 'n, 'c) status
      | GenChange : ('a, 'b, 'n, 'c) status
      | CleanBeforeDive : ('a, 'b, nz, 'c) status
      | CleanAfterDive : 'b -> ('a, 'b, nz, nz) status
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
          main = Kcas.ref (CNode { bmp = 0; array = [||] });
          gen = Kcas.ref (object end);
        };
    }

  let rec clear t =
    let startgen = Kcas.get t.root.gen in
    let empty_mnode = CNode { bmp = 0; array = [||] } in
    if not @@ gen_dcss t.root (Kcas.get t.root.main) empty_mnode startgen then
      clear t

  (** At each level in the trie, we only use 5 bits of the hash. *)
  let lvl_offset = 5

  (** 2^5 - 1 = 31 = 0x1F*)
  let lvl_mask = 0x1F

  let hash_to_flag lvl hash =
    if lvl > Sys.int_size then None
    else
      let shifted = hash lsr lvl in
      let relevant = shifted land lvl_mask in
      Some (1 lsl relevant)

  (** {ul {- [flag] is a single bit flag (never 0)}
          {- [pos] is an index in the array, hence it satisfies 0 <= pos <= popcount bitmap}} *)
  let flagpos lvl bitmap hash =
    match hash_to_flag lvl hash with
    | Some flag ->
        let pos = pred flag land bitmap |> popcount in
        (flag, pos)
    | None -> failwith "Maximum depth reached but flagpos was still used???"

  (* This function assumes l_filtered is sorted and it only contains valid indexes *)
  let remove_from_bitmap bmp l_filtered =
    let rec aux cursor bmp l_filtered seen =
      if cursor >= Sys.int_size then bmp
      else
        let bit = 1 lsl cursor in
        let flag = bit land bmp in
        match (flag, l_filtered) with
        | _, [] -> bmp
        | 0, _ -> aux (cursor + 1) bmp l_filtered seen
        | _, x :: xs when x = seen ->
            aux (cursor + 1) (bmp lxor flag) xs (seen + 1)
        | _ -> aux (cursor + 1) bmp l_filtered (seen + 1)
    in
    aux 0 bmp l_filtered 0

  let resurrect i = function
    | TNode None | LNode [] -> None
    | TNode (Some l) | LNode [ l ] -> Some (Leaf l)
    | _ -> Some i

  let vertical_compact : type n. ('a, n) cNode -> n kind -> ('a, n) mainNode =
   fun cnode lvl ->
    match lvl with
    | Z -> CNode cnode
    | NZ -> (
        match Array.length cnode.array with
        | 0 -> TNode None
        | 1 -> (
            match cnode.array.(0) with
            | Leaf leaf -> TNode (Some leaf)
            | _ -> CNode cnode)
        | _ -> CNode cnode)

  let horizontal_compact cnode lvl =
    let array, l_filtered =
      Array.filter_map
        (function
          | Leaf _ as l -> Some l
          | INode i as inner -> resurrect inner (Kcas.get i.main))
        cnode.array
    in
    let bmp = remove_from_bitmap cnode.bmp l_filtered in
    vertical_compact { array; bmp } lvl

  let clean i old_m cnode lvl startgen =
    let new_cnode = horizontal_compact cnode lvl in
    ignore @@ gen_dcss i old_m new_cnode startgen

  let cnode_with_insert cnode leaf flag pos =
    let new_bitmap = cnode.bmp lor flag in
    let new_array = Array.insert cnode.array pos (Leaf leaf) in
    { bmp = new_bitmap; array = new_array }

  let cnode_with_update cnode branch pos =
    let array = Array.copy cnode.array in
    array.(pos) <- branch;
    { cnode with array }

  let cnode_with_delete cnode flag pos =
    let bmp = cnode.bmp lxor flag in
    let array = Array.remove cnode.array pos in
    { bmp; array }

  (** [regen i old_m cnode pos child_main new_gen] updates the generation of the immediate child
      [cnode.array.(pos)] of [i] to [new_gen].
      Fails if [i]'s generation isn't equal to [new_gen].

      We volontarily do not update the generations of deeper INodes, as this is a lazy algorithm.
      TODO: investigate if this is really wise. *)
  let regenerate i old_m cnode pos child_main new_gen =
    let new_cnode =
      cnode_with_update cnode
        (INode { main = Kcas.ref child_main; gen = Kcas.ref new_gen })
        pos
    in
    gen_dcss i old_m (CNode new_cnode) new_gen

  let find key t =
    let hash = H.hash key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type n. ('a, n) iNode -> n kind -> int -> ('a, _, n, z) status =
       fun i k lvl ->
        match Kcas.get i.main with
        | TNode _ -> CleanBeforeDive
        | LNode ([] | [ _ ]) -> CleanBeforeDive
        | CNode cnode as cn -> (
            let flag, pos = flagpos lvl cnode.bmp hash in
            if flag land cnode.bmp = 0 then raise Not_found
            else
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    match aux inner NZ (lvl + lvl_offset) with
                    | CleanBeforeDive ->
                        clean i cn cnode k startgen;
                        aux i k lvl
                    | (Alright _ | GenChange) as other -> other
                  else if
                    regenerate i cn cnode pos (Kcas.get inner.main) startgen
                  then aux i k lvl
                  else GenChange
              | Leaf leaf ->
                  if H.equal leaf.key key then Alright leaf.value
                  else raise Not_found)
        | LNode lst ->
            let leaf = List.find (fun l -> H.equal l.key key) lst in
            Alright leaf.value
      in
      match aux t.root Z 0 with Alright v -> v | GenChange -> loop ()
    in
    loop ()

  let find_opt key t = try Some (find key t) with Not_found -> None
  let mem key t = Option.is_some (find_opt key t)

  let rec branch_of_pair :
      type n. 'a leaf * int -> 'a leaf * int -> int -> gen -> ('a, n) branch =
   fun (l1, h1) (l2, h2) lvl gen ->
    let flag1 = hash_to_flag lvl h1 in
    let flag2 = hash_to_flag lvl h2 in
    let new_main_node =
      match (flag1, flag2) with
      | Some flag1, Some flag2 ->
          let bmp = flag1 lor flag2 in
          let array =
            match compare flag1 flag2 with
            | 0 ->
                (* Collision on this level, we need to go deeper *)
                [| branch_of_pair (l1, h1) (l2, h2) (lvl + lvl_offset) gen |]
            | 1 -> [| Leaf l2; Leaf l1 |]
            | _ -> [| Leaf l1; Leaf l2 |]
          in
          CNode { bmp; array }
      | _ ->
          (* Maximum depth reached, it's a full hash collision. We just dump everything into a list. *)
          LNode [ l1; l2 ]
    in
    INode { main = Kcas.ref new_main_node; gen = Kcas.ref gen }

  let update key f t =
    let hash = H.hash key in
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type n. ('a, n) iNode -> n kind -> int -> (unit, unit, n, nz) status =
       fun i k lvl ->
        match Kcas.get i.main with
        | TNode _ -> CleanBeforeDive
        | LNode ([] | [ _ ]) -> CleanBeforeDive
        | CNode cnode as cn -> (
            let flag, pos = flagpos lvl cnode.bmp hash in
            if flag land cnode.bmp = 0 then
              (* No flag collision, the key isn't in the map. *)
              match f None with
              | Some value ->
                  (* We need to insert it. *)
                  let new_cnode =
                    cnode_with_insert cnode { key; value } flag pos
                  in
                  if gen_dcss i cn (CNode new_cnode) startgen then Alright ()
                  else GenChange
              | None -> Alright ()
            else
              match cnode.array.(pos) with
              | INode inner ->
                  if Kcas.get inner.gen = startgen then
                    match aux inner NZ (lvl + lvl_offset) with
                    | CleanBeforeDive ->
                        clean i cn cnode k startgen;
                        aux i k lvl
                    | CleanAfterDive () ->
                        clean i cn cnode k startgen;
                        Alright ()
                    | (Alright _ | GenChange) as other -> other
                  else if
                    regenerate i cn cnode pos (Kcas.get inner.main) startgen
                  then aux i k lvl
                  else GenChange
              | Leaf l -> (
                  if H.equal l.key key then
                    match f (Some l.value) with
                    | Some value ->
                        (* We found a value to be updated. *)
                        let new_cnode =
                          cnode_with_update cnode (Leaf { key; value }) pos
                        in
                        if gen_dcss i cn (CNode new_cnode) startgen then
                          Alright ()
                        else GenChange
                    | None ->
                        (* We need to remove this value *)
                        let new_cnode = cnode_with_delete cnode flag pos in
                        let compacted = vertical_compact new_cnode k in
                        if gen_dcss i cn compacted startgen then
                          match compacted with
                          | TNode _ -> CleanAfterDive ()
                          | _ -> Alright ()
                        else GenChange
                  else
                    match f None with
                    | Some value ->
                        (* We create a new entry colliding with a leaf, so we create a new level. *)
                        let new_pair =
                          branch_of_pair
                            (l, H.hash l.key)
                            ({ key; value }, hash)
                            (lvl + lvl_offset) startgen
                        in
                        let new_cnode = cnode_with_update cnode new_pair pos in
                        if gen_dcss i cn (CNode new_cnode) startgen then
                          Alright ()
                        else GenChange
                    (* The key isn't in the map. *)
                    | None -> Alright ()))
        | LNode lst as ln ->
            let rec update_list = function
              | [] -> (
                  match f None with
                  | None -> []
                  | Some v -> [ { key; value = v } ])
              | x :: xs ->
                  if H.equal x.key key then
                    match f (Some x.value) with
                    | Some v -> { key; value = v } :: xs
                    | None -> xs
                  else x :: update_list xs
            in
            let new_list = update_list lst in
            let mainnode, needs_cleaning =
              match new_list with
              | [] -> (TNode None, true)
              | [ l ] -> (TNode (Some l), true)
              | _ -> (LNode new_list, false)
            in
            if gen_dcss i ln mainnode startgen then
              if needs_cleaning then CleanAfterDive () else Alright ()
            else GenChange
      in
      match aux t.root Z 0 with Alright () -> () | GenChange -> loop ()
    in
    loop ()

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
    match Kcas.get t.root.main with CNode cnode -> cnode.bmp = 0

  let filter_map_inplace f t =
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type n. ('a, n) iNode -> n kind -> (('a, n) iNode, _, n, nz) status =
       fun i k ->
        match Kcas.get i.main with
        | TNode _ -> CleanBeforeDive
        | LNode ([] | [ _ ]) -> CleanBeforeDive
        | CNode cnode as cn -> (
            let rec inner_loop :
                ('a, n) branch list ->
                int list ->
                int ->
                (('a, n) mainNode, _, nz, z) status =
             fun l_mapped l_filtered pos ->
              if pos < 0 then
                match (l_mapped, k) with
                | [], NZ -> Alright (TNode None)
                | [ Leaf l ], NZ -> Alright (TNode (Some l))
                | _ ->
                    let array = Array.of_list l_mapped in
                    let bmp = remove_from_bitmap cnode.bmp l_filtered in
                    Alright (CNode { bmp; array })
              else
                match cnode.array.(pos) with
                | INode inner ->
                    if Kcas.get inner.gen = startgen then
                      match aux inner NZ with
                      | Alright inode ->
                          inner_loop (INode inode :: l_mapped) l_filtered
                            (pos - 1)
                      | CleanAfterDive (Some leaf) ->
                          inner_loop (Leaf leaf :: l_mapped) l_filtered (pos - 1)
                      | CleanAfterDive None ->
                          inner_loop l_mapped (pos :: l_filtered) (pos - 1)
                      | (GenChange | CleanBeforeDive) as other -> other
                    else GenChange
                | Leaf { key; value } -> (
                    match f key value with
                    | Some value ->
                        inner_loop
                          (Leaf { key; value } :: l_mapped)
                          l_filtered (pos - 1)
                    | None -> inner_loop l_mapped (pos :: l_filtered) (pos - 1))
            in
            match inner_loop [] [] (Array.length cnode.array - 1) with
            | Alright (TNode l as tnode) ->
                if gen_dcss i cn tnode startgen then CleanAfterDive l
                else GenChange
            | Alright mainnode ->
                if gen_dcss i cn mainnode startgen then Alright i else GenChange
            | CleanBeforeDive ->
                clean i cn cnode k startgen;
                aux i k
            | GenChange -> GenChange)
        | LNode list as ln ->
            let new_list =
              List.filter_map
                (fun { key; value } ->
                  match f key value with
                  | Some value -> Some { key; value }
                  | None -> None)
                list
            in
            let mainnode, problem_leaf =
              match new_list with
              | [] -> (TNode None, Some None)
              | [ l ] -> (TNode (Some l), Some (Some l))
              | _ -> (LNode new_list, None)
            in
            if gen_dcss i ln mainnode startgen then
              match problem_leaf with
              | Some res -> CleanAfterDive res
              | None -> Alright i
            else GenChange
      in
      match aux t.root Z with Alright _ -> () | GenChange -> loop ()
    in
    loop ()

  let map f t =
    let rec loop () =
      let startgen = Kcas.get t.root.gen in
      let rec aux :
          type n. ('a, n) iNode -> n kind -> (('b, n) iNode, _, n, z) status =
       fun i k ->
        match Kcas.get i.main with
        | TNode _ -> CleanBeforeDive
        | LNode ([] | [ _ ]) -> CleanBeforeDive
        | CNode cnode as cn -> (
            let rec inner_loop :
                ('b, n) branch list -> int -> (('b, n) iNode, _, n, z) status =
             fun lst pos ->
              if pos < 0 then
                let array = Array.of_list lst in
                Alright
                  {
                    main = Kcas.ref (CNode { cnode with array });
                    gen = Kcas.ref startgen;
                  }
              else
                match cnode.array.(pos) with
                | Leaf { key; value } ->
                    inner_loop
                      (Leaf { key; value = f key value } :: lst)
                      (pos - 1)
                | INode inner ->
                    if Kcas.get inner.gen = startgen then (
                      match aux inner NZ with
                      | Alright inode ->
                          inner_loop (INode inode :: lst) (pos - 1)
                      | GenChange -> GenChange
                      | CleanBeforeDive ->
                          clean i cn cnode k startgen;
                          aux i k)
                    else if
                      regenerate i cn cnode pos (Kcas.get inner.main) startgen
                    then aux i k
                    else GenChange
            in
            match inner_loop [] (Array.length cnode.array - 1) with
            | CleanBeforeDive ->
                clean i cn cnode k startgen;
                aux i k
            | other -> other)
        | LNode list ->
            let new_list =
              List.map
                (function { key; value } -> { key; value = f key value })
                list
            in
            Alright
              { main = Kcas.ref (LNode new_list); gen = Kcas.ref startgen }
      in
      match aux t.root Z with Alright m -> m | GenChange -> loop ()
    in
    { root = loop () }

  let rec reduce f ?(short_circuiting = fun _ -> false) init t =
    let startgen = Kcas.get t.root.gen in
    let rec aux : type n. 'b -> ('a, n) iNode -> n kind -> ('b, _, n, z) status
        =
     fun acc i k ->
      match Kcas.get i.main with
      | TNode _ -> CleanBeforeDive
      | LNode ([] | [ _ ]) -> CleanBeforeDive
      | CNode cnode as cn -> (
          let rec inner_loop : _ -> int -> (_, _, n, z) status =
           fun inner_acc pos ->
            if pos < 0 then Alright inner_acc
            else
              match cnode.array.(pos) with
              | Leaf { key; value } ->
                  let elem = f key value inner_acc in
                  if short_circuiting elem then Alright elem
                  else inner_loop elem (pos - 1)
              | INode inner ->
                  if Kcas.get inner.gen = startgen then (
                    match aux inner_acc inner NZ with
                    | Alright elem ->
                        if short_circuiting elem then Alright elem
                        else inner_loop elem (pos - 1)
                    | GenChange -> GenChange
                    | CleanBeforeDive ->
                        clean i cn cnode k startgen;
                        aux acc i k)
                  else if
                    regenerate i cn cnode pos (Kcas.get inner.main) startgen
                  then aux acc i k
                  else GenChange
          in
          match inner_loop acc (Array.length cnode.array - 1) with
          | CleanBeforeDive ->
              clean i cn cnode k startgen;
              aux acc i k
          | other -> other)
      | LNode list ->
          let rec list_loop inner_acc = function
            | [] -> Alright inner_acc
            | { key; value } :: xs ->
                let elem = f key value inner_acc in
                if short_circuiting elem then Alright elem
                else list_loop elem xs
          in
          list_loop acc list
    in
    match aux init t.root Z with
    | Alright res -> res
    | GenChange -> reduce f ~short_circuiting init t

  let exists pred =
    reduce (fun k v _ -> pred k v) ~short_circuiting:Fun.id false

  let for_all pred = reduce (fun k v _ -> pred k v) ~short_circuiting:not true
  let iter f = reduce (fun k v _ -> f k v) ()
  let fold f t init = reduce f init t
  let size t = reduce (fun _ _ sum -> sum + 1) 0 t

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
      Printf.fprintf oc "\tC%d [shape=record label=\"<bmp> 0x%X" !ic cnode.bmp;
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
