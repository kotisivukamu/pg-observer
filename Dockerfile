FROM golang:1.26-alpine AS build

WORKDIR /src
COPY pg-observer/go.mod pg-observer/go.sum ./
RUN go mod download
COPY pg-observer/main.go .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /pg-observer .

FROM alpine:3.21
COPY --from=build /pg-observer /pg-observer
CMD ["/pg-observer"]
