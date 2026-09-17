FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN go build -o /inorbit ./cmd/inorbit

FROM alpine:3.20
RUN adduser -D inorbit
USER inorbit
COPY --from=build /inorbit /usr/local/bin/inorbit
EXPOSE 8080
ENTRYPOINT ["inorbit"]
