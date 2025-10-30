open Eio.Std
open Utils

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
                    | Body.String m ->
                        Some (String.length (m body) |> Int64.of_int)
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

type address =
  | Https of Uri.t * Eio.Net.Sockaddr.stream
  | Plain of Uri.t * Eio.Net.Sockaddr.stream

let address_of_uri net uri =
    match Uri.scheme uri with
    | Some "httpunix" ->
        (* FIXME: while there is no standard, http+unix seems more widespread *)
        Plain (uri, unix_address uri)
    | Some "http" -> Plain (uri, tcp_address ~net uri)
    | Some "https" -> Https (uri, tcp_address ~net uri)
    | x ->
       Fmt.failwith "Unknown scheme %a"
         Fmt.(option ~none:(any "None") Dump.string)
         x

let socket_of_address ~sw net https address = match address with
  | Plain (_, addr) -> (Eio.Net.connect ~sw net addr :> S.connection) 
  | Https (uri, addr) -> 
     match https with
     | Some wrap -> wrap uri @@ Eio.Net.connect ~sw net addr
     | None -> Fmt.failwith "HTTPS not enabled (for %a)" Uri.pp uri

let make ~https net : S.t =
  let net = (net :> [ `Generic ] Eio.Net.ty r) in
  let https =
    (https
      :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
         option)
  in
  fun ~sw uri ->
  address_of_uri net uri
  |> socket_of_address ~sw net https
