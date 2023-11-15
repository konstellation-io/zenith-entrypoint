FROM golang:1.21-alpine3.17 as builder

ENV CGO_ENABLED=0

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . .

RUN go build -o entrypoint main.go

FROM alpine:3.17

ENV USER=kai
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

RUN mkdir -p /var/log/app

WORKDIR /app
COPY --from=builder /app/entrypoint .
COPY config.yaml .
RUN chown -R kai:0 /app \
    && chmod -R g+w /app \
    && chown -R kai:0 /var/log/app \
    && chmod -R g+w /var/log/app

USER kai

CMD ["sh","-c","/app/entrypoint 2>&1 | tee -a /var/log/app/app.log"]
