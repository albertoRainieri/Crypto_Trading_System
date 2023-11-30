#!/bin/bash

podman machine stop && podman machine set --rootful && podman machine set --cpus 6 --memory 12288 && podman machine start
ssh -fnNT -L/tmp/podman.sock:/run/user/501/podman/podman.sock -i ~/.ssh/podman-machine-default ssh://core@localhost:50232 -o StreamLocalBindUnlink=yes
export DOCKER_HOST='unix:///Users/albertorainieri/.local/share/containers/podman/machine/qemu/podman.sock'
docker-compose up -d mongo mongo-express backend tracker
sleep 5
docker-compose up -d nginx
docker-compose up backend tracker