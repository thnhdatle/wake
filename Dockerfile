# syntax=docker/dockerfile:1
# SPDX-License-Identifier: Apache-2.0
FROM golang:1.27.3@sha256:f24164268fc1e741bbdc0066dbadcf1c8426c5a8489f0729c02c3905a53032fd AS build
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN mkdir -p /out/var/lib/wake \
 && CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/thnhdatle/wake/internal/platform/buildinfo.Version=${VERSION} \
      -X github.com/thnhdatle/wake/internal/platform/buildinfo.Commit=${COMMIT} \
      -X github.com/thnhdatle/wake/internal/platform/buildinfo.Date=${DATE}" \
    -o /out/wake ./cmd/wake

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/wake /wake
COPY --from=build --chown=65532:65532 /out/var/lib/wake /var/lib/wake
USER 65532:65532
EXPOSE 9093
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 CMD ["/wake","healthcheck"]
ENTRYPOINT ["/wake"]
CMD ["run"]
