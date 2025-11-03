# Cohttp-eio Examples

This directory contains examples illustrating different modes of use of the
cohttp-eio package.

## [`client_proxy.ml`](./client_proxy.ml)

This executable shows an example of how to set up proxying for client requests.

## Prerequisites

The following usage examples assumes

- you are working in root directory of this project,
- you have installed [tinyproxy](https://github.com/tinyproxy/tinyproxy),
- and that you have launched tinyproxy with

  ``` sh
  tinyproxy -d -c cohttp-eio/examples/tinyproxy.conf
  ```

### Direct proxy (for http requests)

``` sh
dune exec cohttp-eio/examples/client_proxy.exe -- --http-proxy=http://127.0.0.1:8888 http://detectportal.firefox.com/success.txt
```


### Tunneling proxy (for https requests)

``` sh
dune exec cohttp-eio/examples/client_proxy.exe -- --http-proxy=http://127.0.0.1:8888 https://detectportal.firefox.com/success.txt
```
