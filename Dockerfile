FROM golang:1.18-alpine as development

ENV CGO_ENABLED=1

WORKDIR /go/src/app

RUN apk update && apk add git
RUN go install github.com/cespare/reflex@latest
COPY . .
RUN go build -o bigquery-tool main.go

CMD ["reflex", "-c", "./reflex.conf"]

FROM alpine AS build
WORKDIR /opt/
COPY --from=development /go/src/app/bigquery-tool bigquery
ENTRYPOINT ["/opt/bigquery"]