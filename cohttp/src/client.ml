let src = Logs.Src.create "cohttp.client" ~doc:"Cohttp Client module"

module Log = (val Logs.src_log src)

(** The [Client] module is a collection of convenience functions for
    constructing and processing requests. *)
module type BASE = sig
  type +'a io
  type 'a with_context
  type body

  val map_context : 'a with_context -> ('a -> 'b) -> 'b with_context

type call =
  ?headers:Http.Header.t ->
  ?body:Body.t ->
  ?absolute_form:bool ->
  Http.Method.t ->
  Uri.t ->
  (Cohttp.Response.t * Body.t) Lwt.t
(** [call ?headers ?body method uri] Function type used to handle http requests

    @return
      [(response, response_body)] [response_body] is not buffered, but stays on
      the wire until consumed. It must therefore be consumed in a timely manner.
      Otherwise the connection would stay open and a file descriptor leak may be
      caused. Following responses would get blocked. Functions in the {!Body}
      module can be used to consume [response_body]. Use {!Body.drain_body} if
      you don't consume the body by other means.

    Leaks are detected by the GC and logged as debug messages, these can be
    enabled activating the debug logging. For example, this can be done as
    follows in [cohttp-lwt-unix]

    {[
      Cohttp_lwt_unix.Debug.activate_debug ();
      Logs.set_level (Some Logs.Warning)
    ]}

    @raise {!Connection.Retry}
      on recoverable errors like the remote endpoint closing the connection
      gracefully. Even non-idempotent requests are guaranteed to not have been
      processed by the remote endpoint and should be retried. But beware that a
      [`Stream] [body] may have been consumed. *)

  val call :
    (?headers:Http.Header.t ->
    ?body:body ->
    ?chunked:bool ->
    Http.Method.t ->
    Uri.t ->
    (Http.Response.t * body) io)
    with_context
  (** [call ?headers ?body ?chunked meth uri]

      @return
        [(response, response_body)] Consume [response_body] in a timely fashion.
        Please see {!val:call} about how and why.
      @param chunked
        use chunked encoding if [true]. The default is [false] for compatibility
        reasons. *)
end

module type S = sig
  include BASE

  val head :
    (?headers:Http.Header.t -> Uri.t -> Http.Response.t io) with_context

  val get :
    (?headers:Http.Header.t -> Uri.t -> (Http.Response.t * body) io)
    with_context

  val delete :
    (?body:body ->
    ?chunked:bool ->
    ?headers:Http.Header.t ->
    Uri.t ->
    (Http.Response.t * body) io)
    with_context

  val post :
    (?body:body ->
    ?chunked:bool ->
    ?headers:Http.Header.t ->
    Uri.t ->
    (Http.Response.t * body) io)
    with_context

  val put :
    (?body:body ->
    ?chunked:bool ->
    ?headers:Http.Header.t ->
    Uri.t ->
    (Http.Response.t * body) io)
    with_context

  val patch :
    (?body:body ->
    ?chunked:bool ->
    ?headers:Http.Header.t ->
    Uri.t ->
    (Http.Response.t * body) io)
    with_context
end

module Make (Base : BASE) (IO : S.IO with type 'a t = 'a Base.io) = struct
  include Base
  open IO

  let call =
    map_context call (fun call ?headers ?body ?chunked meth uri ->
        let () = Log.info (fun m -> m "%a %a" Http.Method.pp meth Uri.pp uri) in
        call ?headers ?body ?chunked meth uri)

  let delete =
    map_context call (fun call ?body ?chunked ?headers uri ->
        call ?body ?chunked ?headers `DELETE uri)

  let get = map_context call (fun call ?headers uri -> call ?headers `GET uri)

  let head =
    map_context call (fun call ?headers uri ->
        call ?headers `HEAD uri >>= fun (response, _body) -> return response)

  let patch =
    map_context call (fun call ?body ?chunked ?headers uri ->
        call ?body ?chunked ?headers `PATCH uri)

  let post =
    map_context call (fun call ?body ?chunked ?headers uri ->
        call ?body ?chunked ?headers `POST uri)

  let put =
    map_context call (fun call ?body ?chunked ?headers uri ->
        call ?body ?chunked ?headers `PUT uri)
end
