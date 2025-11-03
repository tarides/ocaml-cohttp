open Eio.Std

type t = S.t

let cache = Atomic.make Connection_cache.No_cache.(call (create ()))
let set_cache c = Atomic.set cache c

include
  Cohttp.Generic.Client.Make
    (struct
      type 'a io = 'a
      type body = Body.t
      type 'a with_context = S.t -> sw:Eio.Switch.t -> 'a

      let map_context v f t ~sw = f (v t ~sw)

      let call t ~sw ?headers ?body ?(chunked = false) meth uri =
        (Atomic.get cache) t ~sw ?headers ?body ~chunked ~absolute_form:false
          meth uri
    end)
    (Io.IO)

let make_generic fn = (fn :> S.t)

let unix_address uri =
  match Uri.host uri with
  | Some path -> `Unix path
  | None -> Fmt.failwith "no host specified (in %a)" Uri.pp uri

let tcp_address ~net uri =
  let service =
    match Uri.port uri with
    | Some port -> Int.to_string port
    | _ -> Uri.scheme uri |> Option.value ~default:"http"
  in
  match
    Eio.Net.getaddrinfo_stream ~service net
      (Uri.host_with_default ~default:"localhost" uri)
  with
  | ip :: _ -> ip
  | [] -> failwith "failed to resolve hostname"

let make ~https net : S.t =
  let net = (net :> [ `Generic ] Eio.Net.ty r) in
  let https =
    (https
      :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
         option)
  in
  fun ~sw uri ->
    let addr = Address.of_uri net uri in
    let socket = Address.to_socket ~sw net https addr in
    (Address.socketaddr addr, socket)

let call_on_socket ~sw ?headers ?body ?(chunked = false) meth uri socket :
    (Http.Response.t * body) io =
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
        | Some body -> Utils.flow_to_writer body writer Io.Request.write_body)
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
            Utils.flow_of_reader (fun () -> Io.Response.read_body_chunk reader)
          in
          (response, body))

type client =
  sw:Eio.Switch.t ->
  Uri.t ->
  (S.connection -> (Http.Response.t * body) io) ->
  (Http.Response.t * body) io

type cache =
  sw:Eio.Switch.t ->
  Uri.t ->
  (S.connection -> (Http.Response.t * body) io) ->
  (Http.Response.t * body) io

let cache () : cache = raise (Failure "TODO")

let make' ~https net : client =
  let net = (net :> [ `Generic ] Eio.Net.ty r) in
  let https =
    (https
      :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
         option)
  in
  fun ~sw uri call ->
    let with_conn_cache = cache () in
    (* let addr = Address.of_uri net uri in *)
    with_conn_cache ~sw uri call
(* let socket = Address.to_socket ~sw net https addr in *)
(* f socket *)

let call' (client : client) ~sw ?headers ?body ?(chunked = false) meth uri =
  client ~sw uri @@ call_on_socket ~sw ?headers ?body ~chunked meth uri
