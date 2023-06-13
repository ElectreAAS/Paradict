open Lin

module FOO = Paradict.Make (struct
  type t = string

  let equal = ( = )
  let hash s = Hashtbl.hash s
end)

module Basic_test = struct
  type t = int FOO.t

  let init () = FOO.create ()
  let cleanup _ = ()

  let api =
    [
      val_ "clear" FOO.clear (t @-> returning unit);
      val_ "find_opt" FOO.find_opt (string @-> t @-> returning (option int));
      val_ "mem" FOO.mem (string @-> t @-> returning bool);
      val_ "add" FOO.add (string @-> int @-> t @-> returning unit);
      val_ "remove" FOO.remove (string @-> t @-> returning unit);
      val_ "copy" FOO.copy (t @-> returning_ t);
      val_ "is_empty" FOO.is_empty (t @-> returning bool);
      val_ "size" FOO.size (t @-> returning int);
    ]
end

module Run = Lin_domain.Make (Basic_test)

let () =
  QCheck_base_runner.run_tests_main [ Run.lin_test ~count:1_000 ~name:"Foo" ]
