FROM golang:1.24.1-alpine

WORKDIR /app

RUN apk add --no-cache

COPY go.mod go.sum ./

RUN go mod download

COPY .. .



RUN go build -o /reader ./main.go

EXPOSE 8080

CMD ["/reader"]
