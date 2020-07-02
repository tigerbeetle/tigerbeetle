FROM node:12.16.0-alpine as builder
WORKDIR /opt/tiger-beetle

RUN apk add --no-cache -t build-dependencies python make gcc g++ libtool autoconf automake

COPY package.json package-lock.json* /opt/tiger-beetle/

RUN npm install

FROM node:12.16.0-alpine
WORKDIR /opt/tiger-beetle

COPY --from=builder /opt/tiger-beetle .

WORKDIR /opt/tiger-beetle

COPY scripts ./scripts
COPY index.js .
COPY result.js .
COPY journal.js .
COPY stress.js .

RUN dd < /dev/zero bs=1048576 count=256 > journal
RUN ./scripts/create-transfers

EXPOSE 30000

CMD ["npm", "run", "start"]