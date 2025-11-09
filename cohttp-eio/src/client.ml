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

(* let unix_address uri = *)
(*   match Uri.host uri with *)
(*   | Some path -> `Unix path *)
(*   | None -> Fmt.failwith "no host specified (in %a)" Uri.pp uri *)

(* let tcp_address ~net uri = *)
(*   let service = *)
(*     match Uri.port uri with *)
(*     | Some port -> Int.to_string port *)
(*     | _ -> Uri.scheme uri |> Option.value ~default:"http" *)
(*   in *)
(*   match *)
(*     Eio.Net.getaddrinfo_stream ~service net *)
(*       (Uri.host_with_default ~default:"localhost" uri) *)
(*   with *)
(*   | ip :: _ -> ip *)
(*   | [] -> failwith "failed to resolve hostname" *)

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


let cache_key = Eio.Fiber.create_key ()

let with_cache f =
  Eio.Fiber.with_binding cache_key (Cache.Cache.create ()) f

let get_cache () : Cache.Cache.t option =
  Eio.Fiber.get cache_key

(* let make_tunnel ~sw ~net ~fwd_uri socket  = *)
(*   let socket = Eio.Net.connect ~sw net socket in *)
(*   let resp, _ = call_on_socket ~sw `CONNECT fwd_uri socket in *)

let make_tunnel ~sw https uri socket =
  traceln "Creating tunnel with proxy";
  let resp, _ = call_on_socket ~sw `CONNECT uri socket in
  match Http.Response.status resp with
  | #Http.Status.success ->
     traceln "Connection established. Switching to tls.";
     https uri socket
  | _ -> Fmt.failwith "Proxy could not form tunnel for %a" Uri.pp uri
  
let connect_to_addr_on_socket ~sw ~proxy ~https remote_addr socket =
  match (remote_addr : Address.t) with
  | Plain (_, _) ->
     traceln "Making plain call";
     (socket :> S.connection)
  | Https (https_uri, _) ->
     match https with
     | None -> Fmt.failwith "HTTPS not enabled (for %a)" Uri.pp https_uri
     | Some wrap ->
        (* TODO: We are issuing double connects !!! We need to move the proxy logic into the cache to avoid this mess *)
        match proxy with
        | None -> wrap https_uri socket
        | Some _ -> make_tunnel ~sw wrap https_uri socket
  
(* TODO: Read proxy from envvar *)
let make' ?proxy ~https net : client =
  let net = (net :> [ `Generic ] Eio.Net.ty r) in
  let https =
    (https
     :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
          option)
  in
  fun ~sw uri call ->
  let cache = get_cache () in
  let socket_addr, remote_addr, cache_addr =
    match proxy with
    | None ->
       let addr = Address.of_uri net uri in
       addr, addr, addr
    | Some proxy_uri ->
       let remote_addr = Address.of_uri net uri in
       match remote_addr with
       | Https _ -> (Address.of_uri net proxy_uri), remote_addr, remote_addr
       | Plain _ -> let proxy_addr = Address.of_uri net proxy_uri in proxy_addr, remote_addr, proxy_addr
  in
  match cache with
  | None ->
     traceln "Oneshot call";
     let socket = Eio.Net.connect ~sw net (Address.socketaddr socket_addr) in
     call @@ connect_to_addr_on_socket ~sw ~proxy ~https remote_addr socket
  | Some cache ->
     traceln "Calling with cache";
     match Cache.Cache.get cache (Address.socketaddr remote_addr) with
     | Some conn -> Cache.Connection.use conn (fun socket -> call (socket :> S.connection))
     | None ->
        traceln "Cached connection not found. Opening socket to %a" Eio.Net.Sockaddr.pp (Address.socketaddr socket_addr);
        let socket = Eio.Net.connect ~sw net (Address.socketaddr socket_addr) in
        let socket' = connect_to_addr_on_socket ~sw ~proxy ~https remote_addr socket in
        (* We cannot simply create a new connection and add it, because...?  *)
        Cache.Cache.add cache (Address.socketaddr cache_addr) socket';
        call socket'
(* |> Cache.Connection.use (connect_to_addr_on_socket ~sw ~proxy ~https remote_addr) *)

(* let socket = Address.to_socket ~sw net https addr in *)
(* f socket *)

let call' (client : client) ~sw ?headers ?body ?(chunked = false) meth uri =
  client ~sw uri @@ call_on_socket ~sw ?headers ?body ~chunked meth uri
