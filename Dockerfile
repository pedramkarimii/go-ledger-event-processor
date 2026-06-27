FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

FROM alpine:3.23

RUN addgroup -S app && adduser -S -G app app
WORKDIR /app

COPY --from=build /out/api /app/api
COPY --from=build /out/consumer /app/consumer

USER app
