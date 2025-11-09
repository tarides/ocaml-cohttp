(* TODO open Eio.Std *)
(* open Utils *)
open Eio.Std

module Connection : sig
  type t

  val make : S.connection -> t
  val use : t -> (S.connection -> 'a)  -> 'a
end = struct
  type t =
    { mutex : Eio.Mutex.t
    ; socket : S.connection
    }

  let make socket =
    { mutex = Eio.Mutex.create ()
    ; socket
    }

  let use {socket; mutex} f =
    Eio.Mutex.use_rw ~protect:false mutex (fun () -> f socket)
end

module Cache
  : sig
  type t
  type addr = Eio.Net.Sockaddr.stream
  type conn = Connection.t

  val create : unit -> t
  val get : t -> addr -> Connection.t option
  val add : t -> addr -> S.connection -> unit
end
= struct
  type addr = Eio.Net.Sockaddr.stream
  type conn = Connection.t

  type t = {
    (* We only cache for [stream] sockets, because we only care for what
         we can connect to via [Eio.Net.connect] *)
    hashtbl : (addr, Connection.t) Hashtbl.t;
    mutex : Eio.Mutex.t;
  }

  let create () =
    {
      hashtbl = Hashtbl.create 10 (* TODO What is the right number here? *);
      mutex = Eio.Mutex.create ();
    }

  (* let remove {hashtbl; mutex} addr = *)
  (*   Eio.Mutex.use_rw ~protect:true mutex (fun () -> Hashtbl.remove hashtbl addr) *)

  let get (t : t) addr =
    let protect = false in
    Eio.Mutex.use_rw ~protect t.mutex (fun () ->
        traceln "Looking up connection for %a" Eio.Net.Sockaddr.pp addr;
        Hashtbl.find_opt t.hashtbl addr)

  let add (t : t) addr socket =
    let protect = false in
    Eio.Mutex.use_rw ~protect t.mutex (fun () ->
        traceln "Adding connection for %a" Eio.Net.Sockaddr.pp addr;
        let conn = Connection.make socket in
        Hashtbl.add t.hashtbl addr conn)
end
