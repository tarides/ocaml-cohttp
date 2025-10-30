(* TODO open Eio.Std *)
open Utils

module Connection : sig
  type t

  val create : ?limit:int -> sw:Eio.Switch.t -> S.connection -> t
  (** [create ~sw socket] is a new managed connection for the [socket] backed by
      a forked process attached to [sw].

      The process will be suspended unless it has messages pending in its queue
      from {!val:call}.

      Connections should be terminated when they are no longer needed via
      {!val:close}.

      @param limit
        set the max limit of the connection's message input stream. When the
        input stream is full, attempts to add to the queue will block until
        capacity if freed up. Default: [max_int] *)

  val close : t -> unit

  (* TODO Remove if not needed *)
  (* val send : ?body:Body.t -> request:Http.Request.t -> t -> (Http.Response.t * Body.t) *)
  (** TODO(doc): Send a request and wait for the response *)

  val call :
    headers:Http.Header.t option ->
    body:Body.t option ->
    chunked:bool ->
    absolute_form:bool option ->
    Cohttp.Code.meth ->
    Uri.t ->
    t ->
    Http.Response.t * Body.t

  (** [call ~headers ~body ~chunked ~aboslute_form meth uri conn] sends a new
      request via the connection [conn], blocking until the response is
      received. *)
end = struct
  (* The internal representation of a message to sent to the connection process *)
  type req = {
    body : Body.t option;
    request : Http.Request.t;
    resolver : (Http.Response.t * Body.t) Eio.Promise.u;
  }

  (* Internally, a connection is a forked process that reads from an
     asynchronous stream of messages.

     Values of its principle type are functions for sending messages along the
     stream. The message [None] closes the connection. *)
  type t = req option -> unit

  let create ?(limit = max_int) ~sw socket : t =
    let request_stream : req option Eio.Stream.t = Eio.Stream.create limit in
    (* By giving constructors this send function, we provide a write-only
       stream,  making it impossible for other parts of the program to steal
       messages from the stream. *)
    let send_request req = Eio.Stream.add request_stream req in
    let consume () =
      Eio.Buf_write.with_flow socket @@ fun output ->
      let loop () =
        match Eio.Stream.take request_stream with
        | None ->
            (* Stream "closed", so we terminate *)
            ()
        | Some { request; body; resolver } -> (
            let () =
              Io.Request.write ~flush:false
                (fun writer ->
                  match body with
                  | None -> ()
                  | Some body ->
                      flow_to_writer body writer Io.Request.write_body)
                request output
            in
            let input = Eio.Buf_read.of_flow ~max_size:max_int socket in
            match Io.Response.read input with
            | `Eof -> failwith "connection closed by peer"
            | `Invalid reason -> failwith reason
            | `Ok response ->
                let response =
                  match Cohttp.Response.has_body response with
                  | `No -> (response, Eio.Flow.string_source "")
                  | `Yes | `Unknown ->
                      let body =
                        let reader =
                          Io.Response.make_body_reader response input
                        in
                        flow_of_reader (fun () ->
                            Io.Response.read_body_chunk reader)
                      in
                      (response, body)
                in
                Eio.Promise.resolve resolver response)
      in
      loop ()
    in
    Eio.Fiber.fork ~sw consume;
    send_request

  let close (t : t) = t None

  let send ?body ~request (t : t) : (Http.Response.t * Body.t) Eio.Promise.t =
    let promise, resolver = Eio.Promise.create () in
    t (Some { body; request; resolver });
    promise

  let call ~headers ~body ~chunked ~absolute_form meth uri conn =
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
        ?body_length ?absolute_form meth uri
    in
    Eio.Promise.await @@ send ?body ~request conn
end

module No_cache = struct
  type t = unit

  let call () : S.cache_call =
   fun t ~sw ?headers ?body ?(chunked = false) ?absolute_form meth uri ->
    let _addr, socket = t ~sw uri in
    let conn = Connection.create ~sw socket in
    let resp =
      Connection.call ~headers ~body ~chunked ~absolute_form meth uri conn
    in
    Connection.close conn;
    resp

  let create () = ()
end

module Cache = struct
  (* A thread-safe mutable hashtable for caching connections *)
  module Tbl : sig
    type t
    type key = Eio.Net.Sockaddr.stream
    type value = Connection.t

    val create : unit -> t
    val get : key -> t -> value option
    val add : key -> value -> t -> unit
  end = struct
    type key = Eio.Net.Sockaddr.stream
    type value = Connection.t

    type t = {
      (* We only cache for [stream] sockets, because we only care for what
         we can connect to via [Eio.Net.connect] *)
      hashtbl : (key, value) Hashtbl.t;
      mutex : Eio.Mutex.t;
    }

    let create () =
      {
        hashtbl = Hashtbl.create 10 (* TODO What is the right number here? *);
        mutex = Eio.Mutex.create ();
      }

    let get k (t : t) =
      Eio.Mutex.use_ro t.mutex (fun () -> Hashtbl.find_opt t.hashtbl k)

    let add k v (t : t) =
      (*  [protect] tells Eio to ensure the critical section cannot be canceled.
          But canceling adding a connection to the cache is fine in our case, nothing
          bad would come of it. *)
      let protect = false in
      Eio.Mutex.use_rw ~protect t.mutex (fun () ->
          (* We use [replace] over [add] because the latter supports adding
             multiple  values for a key, but we don't currently support caching
             multiple connections for the same endpoint.  *)
          Hashtbl.replace t.hashtbl k v)
  end

  type t = { cache : Tbl.t }

  let create ?keep:_TODO ?retry:_TODO ?parallel:_TODO ?depth:_TODO ?proxy:_TODO
      () =
    { cache = Tbl.create () }

  let call { cache } : S.cache_call =
   fun t ~sw ?headers ?body ?(chunked = false) ?absolute_form meth uri ->
    let addr, socket = t ~sw uri in
    match Tbl.get addr cache with
    | Some conn ->
        Connection.call ~headers ~body ~chunked ~absolute_form meth uri conn
    | None ->
        let conn = Connection.create ~sw socket in
        Tbl.add addr conn cache;
        Connection.call ~headers ~body ~chunked ~absolute_form meth uri conn
end

module StringSet = Set.Make (String)

let tunnel_schemes = StringSet.of_list [ "https" ]

module No_proxy = struct
  type pattern =
    | Name of string
    | Domain of string
    | Ipaddr_prefix of Ipaddr.Prefix.t

  type t = Wildcard | Patterns of pattern list

  let string_equal_caseless a b =
    String.equal (String.lowercase_ascii a) (String.lowercase_ascii b)

  let name_in_domain ~domain ~name =
    match string_equal_caseless domain name with
    | true ->
        (* host example.com matches domain example.com *)
        true
    | false ->
        let suffix = Printf.sprintf ".%s" domain |> String.lowercase_ascii in
        let name = String.lowercase_ascii name in
        String.ends_with ~suffix name

  let is_domain name =
    match name.[0] with
    | '.' -> true
    | _ -> false
    | exception Invalid_argument _ -> false

  let trim_dots s =
    s
    |> String.split_on_char '.'
    |> List.fold_left
         (fun (head, tail) e ->
           match e with
           | "" as e ->
               (* collect in tail *)
               (head, e :: tail)
           | content ->
               (* we got something thats not empty, append tail to head & clear tail *)
               (content :: (tail @ head), []))
         ([], [])
    |> fst
    |> List.rev
    |> String.concat "."

  let parse_pattern pattern =
    match Ipaddr.of_string pattern with
    | Ok addr -> Ipaddr_prefix (Ipaddr.Prefix.of_addr addr)
    | Error _ -> (
        match Ipaddr.Prefix.of_string pattern with
        | Ok prefix -> Ipaddr_prefix prefix
        | Error _ -> (
            let dotless = trim_dots pattern in
            match is_domain pattern with
            | true -> Domain dotless
            | false -> Name dotless))

  let parse_definition s =
    match s with
    | "*" -> Wildcard
    | s ->
        let patterns =
          s
          |> String.split_on_char ','
          |> List.filter_map (function
               | "" -> None
               | pattern -> Some (String.trim pattern))
          |> List.map parse_pattern
        in
        Patterns patterns

  let parse = function
    | None -> Patterns []
    | Some definition -> parse_definition definition

  (** [applicable patterns ~host] is true when the host matches the pattern *)
  let applicable patterns ~host =
    match host with
    | "" -> true
    | host -> (
        match patterns with
        | Wildcard -> true
        | Patterns patterns -> (
            match Ipaddr.of_string host with
            | Ok host_ip ->
                List.exists
                  (function
                    | Name _ | Domain _ -> false
                    | Ipaddr_prefix network -> Ipaddr.Prefix.mem host_ip network)
                  patterns
            | Error _ ->
                List.exists
                  (function
                    | Ipaddr_prefix _ -> false
                    | Name pattern -> string_equal_caseless pattern host
                    | Domain domain ->
                        let name = trim_dots host in
                        name_in_domain ~domain ~name)
                  patterns))
end

module Proxy = struct
  (* TODO: different types of proxies *)
  module Direct = Cache
  module Tunnel = No_cache

  type t = {
    proxies : (string * S.cache_call) list;
    no_proxy : S.cache_call;
    no_proxy_patterns : No_proxy.t;
    direct : S.cache_call option;
    tunnel : S.cache_call option;
  }

  let create ?keep ?retry ?parallel ?depth ?(scheme_proxy = []) ?all_proxy
      ?no_proxy ?proxy_headers:_ ~net:_ () =
    let create_default () =
      Direct.create ?keep ?retry ?parallel ?depth () |> Direct.call
    in
    let no_proxy_patterns = No_proxy.parse no_proxy in
    let no_proxy = create_default () in

    let proxies =
      List.map
        (fun (scheme, _uri) ->
          match StringSet.mem scheme tunnel_schemes with
          | true ->
              let tunnel = Tunnel.create () |> Tunnel.call in
              (scheme, tunnel)
          | false ->
              let direct =
                Direct.create ?keep ?retry ?parallel ?depth () |> Direct.call
              in
              (scheme, direct))
        scheme_proxy
    in
    let direct, tunnel =
      match all_proxy with
      | None -> None, None
      | Some _uri_TODO ->
          let direct = Direct.create ?keep ?retry ?parallel ?depth () |> Direct.call in
          let tunnel = Tunnel.create () |> Tunnel.call in
          (Some direct, Some tunnel)

    in
    { proxies; no_proxy; direct; tunnel; no_proxy_patterns }

  let call (t : t) : S.cache_call =
   fun t' ~sw ?headers ?body ?chunked ?absolute_form meth uri ->
    let proxy =
      match
        No_proxy.applicable t.no_proxy_patterns
          ~host:(Uri.host_with_default ~default:"" uri)
      with
      | true -> None
      | false -> (
          let scheme = Option.value ~default:"" (Uri.scheme uri) in
          match List.assoc scheme t.proxies with
          | proxy -> Some proxy
          | exception Not_found -> (
              match StringSet.mem scheme tunnel_schemes with
              | true -> t.tunnel
              | false -> t.direct))
    in
    match proxy with
    | None -> t.no_proxy ~sw ?headers ?body ?chunked ?absolute_form t' meth uri
    | Some proxy -> proxy ~sw ?headers ?body ?chunked ?absolute_form t' meth uri
end
