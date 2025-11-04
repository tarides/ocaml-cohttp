(* TODO open Eio.Std *)
open Utils
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
  val get : t -> sw:Eio.Switch.t -> net: _ Eio.Net.t -> addr -> Connection.t
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


  let get (t : t) ~sw ~net addr =
    let net = (net :> [ `Generic ] Eio.Net.ty r) in
    let protect = false in
    Eio.Mutex.use_rw ~protect t.mutex (fun () ->
        match Hashtbl.find_opt t.hashtbl addr with
        | Some conn ->
          traceln "Using cached conn";
          conn
        | None ->
          traceln "Creating new conn";
          let conn = Eio.Net.connect ~sw net addr |> Connection.make in
          Hashtbl.add t.hashtbl addr conn;
          (* The lifetime of a socket ends with the switch it's created with, so
             we can also safely remove its entry from the cache *)
          Eio.Switch.on_release sw (fun () -> remove t addr);
          conn)
end

open Eio.Std

type t = sw:Eio.Switch.t -> Uri.t -> S.connection

let make ~https net : t =
  let net = (net :> [ `Generic ] Eio.Net.ty r) in
  let https =
    (https
      :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
         option)
  in
  fun ~sw uri -> Address.of_uri net uri |> Address.to_socket ~sw net https

let call (t : t) ~sw ?headers ?body ?(chunked = false) meth uri =
  let socket = t ~sw uri in
  let body_length =
    if chunked then None
    else
      match body with
      | None -> Some 0L
      | Some (Eio.Resource.T (body, ops)) ->
          let module X = (val Eio.Resource.get ops Eio.Flow.Pi.Source) in
          List.find_map
            (function
              | Body.String m -> Some (String.length (m body) |> Int64.of_int)
              | _ -> None)
            X.read_methods
  in
  let request =
    Cohttp.Request.make_for_client ?headers
      ~chunked:(Option.is_none body_length)
      ?body_length meth uri
  in
  Eio.Buf_write.with_flow socket @@ fun output ->
  let () =
    Eio.Fiber.fork ~sw @@ fun () ->
    Io.Request.write ~flush:false
      (fun writer ->
        match body with
        | None -> ()
        | Some body -> flow_to_writer body writer Io.Request.write_body)
      request output
  in
  let input = Eio.Buf_read.of_flow ~max_size:max_int socket in
  match Io.Response.read input with
  | `Eof -> failwith "connection closed by peer"
  | `Invalid reason -> failwith reason
  | `Ok response -> (
      match Cohttp.Response.has_body response with
      | `No -> (response, Eio.Flow.string_source "")
      | `Yes | `Unknown ->
          let body =
            let reader = Io.Response.make_body_reader response input in
            flow_of_reader (fun () -> Io.Response.read_body_chunk reader)
          in
          (response, body))
