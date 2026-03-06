# hhttps://hub.docker.com/_/node
FROM node:22

LABEL maintainer="ZhongFuze <zhongsigen@outlook.com>"

RUN apt-get update -y && \
    apt-get upgrade -y --force-yes && \
    apt-get install -y --force-yes supervisor

WORKDIR /node_app

COPY package*.json ./

RUN npm install -g npm@10

# npm install --legacy-peer-deps
RUN npm install --only=production --no-audit

COPY . .

ENTRYPOINT ["./run.sh"]
