FROM golang:1.24.1-alpine as build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN go build -o producer main.go

FROM alpine:latest

WORKDIR /app
COPY --from=build /app/producer .

EXPOSE 8081

CMD ["./producer"]
