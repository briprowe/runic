#!/usr/bin/env bash

cmd="/usr/bin/etcd \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://127.0.0.1:2380"

podman run \
       -p 2379:2379 \
       etcd $cmd
