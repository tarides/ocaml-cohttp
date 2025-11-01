(* TODO open Eio.Std *)
open Utils

module Box : sig
  type 'a t

  val make : 'a -> 'a t
  val take : 'a t -> 'a
  val put : 'a t -> 'a -> unit
end = struct
  type 'a t = 'a Eio.Stream.t

  let make x =
    let s = Eio.Stream.create 1 in
    let () = Eio.Stream.add s x in
    s

  let take t = Eio.Stream.take t
  let put t x = Eio.Stream.add t x
end

module Cache : sig
  type t
  type addr = Eio.Net.Sockaddr.stream
  type conn = S.connection

  val create : unit -> t
  val remove_conn : t -> addr -> unit
  val take_conn : t -> addr -> conn option
  val put_conn : t -> conn -> unit
  val add_conn : t -> addr -> conn -> unit
end = struct
  type addr = Eio.Net.Sockaddr.stream
  type conn = S.connection

  type t = {
    (* We only cache for [stream] sockets, because we only care for what
         we can connect to via [Eio.Net.connect] *)
    hashtbl : (addr, conn Box.t) Hashtbl.t;
    mutex : Eio.Mutex.t;
  }

  let create () =
    {
      hashtbl = Hashtbl.create 10 (* TODO What is the right number here? *);
      mutex = Eio.Mutex.create ();
    }

  let take_conn (t : t) addr =
    Eio.Mutex.use_ro t.mutex (fun () ->
        Hashtbl.find_opt t.hashtbl addr |> Option.map Box.take)

  let put_conn (t : t) addr =
    Eio.Mutex.use_ro t.mutex (fun () ->
        Hashtbl.find_opt t.hashtbl addr |> Option.map Box.take)

  let with_existing_conn (t : t) addr f =
    (*  [protect] tells Eio to ensure the critical section cannot be canceled.
          This ensures that a connection will not closed while it is use. *)
    let protect = true in
    Eio.Mutex.use_rw ~protect t.mutex (fun () ->
        (* We use [replace] over [add] because the latter supports adding
             multiple  values for a key, but we don't currently support caching
             multiple connections for the same endpoint.  *)
        match Hashtbl.find_opt t.hashtbl addr with
        | Some conn -> Some (f conn)
        | None -> None)

  let remove_conn (t : t) addr =
    Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
        Hashtbl.remove t.hashtbl addr)

  let with_new_conn ~sw (t : t) addr conn f =
    Eio.Mutex.use_rw ~protect:false t.mutex (fun () ->
        Hashtbl.add t.hashtbl addr conn;
        Eio.Switch.on_release sw (fun () -> remove_conn t addr);
        f conn)
end

open Eio.Std

type t = sw:Eio.Switch.t -> Uri.t -> S.connection

let cache = ref (Cache.create ())

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
