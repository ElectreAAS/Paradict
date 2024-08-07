open Lin

module P = Paradict.Make (struct
  type t = string

  let equal = ( = )
  let hash s = Hashtbl.hash s
end)

module Basic_test = struct
  type t = int P.t

  let init () = P.create ()
  let cleanup _ = ()

  let api =
    [
      val_ "clear" P.clear (t @-> returning unit);
      val_ "add" P.add (string @-> int @-> t @-> returning unit);
      val_ "find_opt" P.find_opt (string @-> t @-> returning (option int));
      val_ "remove" P.remove (string @-> t @-> returning unit);
      val_ "copy" P.copy (t @-> returning_ t);
      val_ "is_empty" P.is_empty (t @-> returning bool);
      val_ "size" P.size (t @-> returning int);
    ]
end

module Run = Lin_domain.Make (Basic_test)

let () =
  QCheck_base_runner.run_tests_main [ Run.lin_test ~count:10 ~name:"Paradict" ]
