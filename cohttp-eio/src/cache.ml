(* TODO open Eio.Std *)
(* open Utils *)
open Eio.Std

module Connection : sig
  type t

  val make : [ `Generic ] Eio.Net.stream_socket_ty r -> t
  val use : ([ `Generic ] Eio.Net.stream_socket_ty r -> 'a) -> t -> 'a
end = struct
  type t =
    { mutex : Eio.Mutex.t
    ; socket : [ `Generic ] Eio.Net.stream_socket_ty r
    }

  let make socket =
    { mutex = Eio.Mutex.create ()
    ; socket
    }

  let use f {socket; mutex} =
    Eio.Mutex.use_rw ~protect:false mutex (fun () -> f socket)
end

module Cache
  : sig
  type t
  type addr = Eio.Net.Sockaddr.stream
  type conn = Connection.t

  val create : unit -> t
  val get : t -> new_socket:(Address.t -> [ `Key of Address.t ] * [ `Generic ] Eio.Net.stream_socket_ty r) -> Address.t -> Connection.t
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

  let remove {hashtbl; mutex} addr =
    Eio.Mutex.use_rw ~protect:true mutex (fun () -> Hashtbl.remove hashtbl addr)

  let get (t : t) ~new_socket addr =
    let net = (net :> [ `Generic ] Eio.Net.ty r) in
    let protect = false in
    Eio.Mutex.use_rw ~protect t.mutex (fun () ->
        let socket_addr, key_addr, uri =
          match proxy, (addr : Address.t) with
          | Some p, Https (uri, u) ->
            (* Socket with https, we cache on HTTPS address, because we set up a dedicated tunnel *)
            Address.socketaddr p, u, uri
          | Some p, Plain (_, _) ->
            (* Socket with http, we cache on socket, because direct proxies can do all requests from same proxy socket  *)
            Address.socketaddr p, Address.socketaddr p, Address.to_uri p
          | None, (Https (uri, u) | Plain (uri, u)) ->
            (* No proxy, we just cache on the address *)
            u, u, uri
        in
        traceln "Looking up connection for %a" Uri.pp uri;
        match Hashtbl.find_opt t.hashtbl key_addr with
        | Some conn ->
          traceln "Using cached conn";
          conn
        | None ->
          traceln "Creating new conn";
          let conn = Eio.Net.connect ~sw net socket_addr |> Connection.make in
          Hashtbl.add t.hashtbl key_addr conn;
          (* The lifetime of a socket ends with the switch it's created with, so
             we can also safely remove its entry from the cache *)
          Eio.Switch.on_release sw (fun () -> remove t key_addr);
          conn)
end
