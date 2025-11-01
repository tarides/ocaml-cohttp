open Cohttp_eio
module Cache = Cohttp_eio.Connection_cache.Proxy

let of_proxy ~scheme maybe_proxy =
  match maybe_proxy with None -> [] | Some proxy -> [ (scheme, proxy) ]

let run_client url all_proxy no_proxy http_proxy https_proxy proxy_auth =
  let scheme_proxy =
    []
    @ of_proxy ~scheme:"http" http_proxy
    @ of_proxy ~scheme:"https" https_proxy
  in
  let proxy_headers =
    Option.map
      (fun credential ->
        Http.Header.init_with "Proxy-Authorization"
          (Cohttp.Auth.string_of_credential credential))
      proxy_auth
  in

  Eio_main.run @@ fun env ->
  let net = env#net in
  let cache =
    Cache.create ?all_proxy ~scheme_proxy ?no_proxy ?proxy_headers ~net ()
  in
  Client.set_cache (Cache.call cache);
  let client = Client.make ~https:None net in
  Eio.Switch.run @@ fun sw ->
  let resp, body = Client.get ~sw client url in
  let () =
  match resp.status with
  | `OK ->
      print_string @@ Eio.Buf_read.(parse_exn take_all) body ~max_size:max_int
  | otherwise -> Fmt.epr "Unexpected HTTP status: %a\n" Http.Status.pp otherwise
  in
  Cache.shutdown cache

let uri_conv =
  let parser s =
    match Uri.of_string s with
    | uri -> Ok uri
    | exception Failure _ -> Error "unable to parse URI"
  in
  let pp ppf u = Fmt.pf ppf "%s" (Uri.to_string u) in
  Cmdliner.Arg.Conv.make ~parser ~pp ~docv:"URI" ()

let credential_conv =
  let parser s =
    match Base64.encode s with
    | Ok s ->
        s |> Fmt.str "Basic %s" |> Cohttp.Auth.credential_of_string |> Result.ok
    | Error (`Msg m) -> Error m
  in
  let pp ppf c = Fmt.pf ppf "%s" (Cohttp.Auth.string_of_credential c) in
  Cmdliner.Arg.Conv.make ~parser ~pp ~docv:"CREDENTIAL" ()

let uri =
  Cmdliner.Arg.(
    required
    & pos 0 (some uri_conv) None
    & info [] ~docv:"URI"
        ~doc:"string of the remote address (e.g. https://ocaml.org)")

let all_proxy =
  let env = Cmdliner.Cmd.Env.info "ALL_PROXY" in
  Cmdliner.Arg.(
    value
    & opt (some uri_conv) None
    & info [ "all-proxy" ] ~env ~docv:"URI" ~doc:"Proxy all through URI")

let no_proxy =
  let env = Cmdliner.Cmd.Env.info "NO_PROXY" in
  Cmdliner.Arg.(
    value
    & opt (some string) None
    & info [ "no-proxy" ] ~env ~docv:"?" ~doc:"???")

let http_proxy =
  let env = Cmdliner.Cmd.Env.info "HTTP_PROXY" in
  Cmdliner.Arg.(
    value
    & opt (some uri_conv) None
    & info [ "http-proxy" ] ~env ~docv:"URI" ~doc:"???")

let https_proxy =
  let env = Cmdliner.Cmd.Env.info "HTTPS_PROXY" in
  Cmdliner.Arg.(
    value
    & opt (some uri_conv) None
    & info [ "https-proxy" ] ~env ~docv:"URI" ~doc:"???")

let proxy_auth =
  Cmdliner.Arg.(
    value
    & opt (some credential_conv) None
    & info [ "proxy-auth" ] ~docv:"CREDENTIAL" ~doc:"Proxy credentials")

let cmd =
  let info =
    let version = Cohttp.Conf.version in
    let doc = "retrieve a remote URI contents" in
    Cmdliner.Cmd.info "client_proxy" ~version ~doc
  in

  let term =
    Cmdliner.Term.(
      const run_client
      $ uri
      $ all_proxy
      $ no_proxy
      $ http_proxy
      $ https_proxy
      $ proxy_auth)
  in
  Cmdliner.Cmd.v info term

let () = exit @@ Cmdliner.Cmd.eval cmd
