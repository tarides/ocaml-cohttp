open Eio.Std

type t =
  | Https of Uri.t * Eio.Net.Sockaddr.stream
  | Plain of Uri.t * Eio.Net.Sockaddr.stream

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

(* TODO: Public *)

let of_uri net uri =
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

let socketaddr = function Https (_, addr) | Plain (_, addr) -> addr

let to_uri = function Https (uri, _) | Plain (uri, _) -> uri

let to_socket ~sw (net : _ Eio.Net.t) https address =
  let https =
    (https
      :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection)
         option)
  in
  match address with
  | Plain (_, addr) -> (Eio.Net.connect ~sw net addr :> S.connection)
  | Https (uri, addr) -> (
      match https with
      | Some wrap -> wrap uri @@ Eio.Net.connect ~sw net addr
      | None -> Fmt.failwith "HTTPS not enabled (for %a)" Uri.pp uri)

(* let to_socket ~sw (net : _ Eio.Net.t) https address = *)
(*   let https = *)
(*     (https *)
(*       :> (Uri.t -> [ `Generic ] Eio.Net.stream_socket_ty r -> S.connection) *)
(*          option) *)
(*   in *)
(*   match address with *)
(*   | Plain (_, addr) -> (Eio.Net.connect ~sw net addr :> S.connection) *)
(*   | Https (uri, addr) -> ( *)
(*       match https with *)
(*       | Some wrap -> wrap uri @@ Eio.Net.connect ~sw net addr *)
(*       | None -> Fmt.failwith "HTTPS not enabled (for %a)" Uri.pp uri) *)
