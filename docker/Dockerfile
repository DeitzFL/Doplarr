FROM clojure:temurin-22-tools-deps AS builder

WORKDIR /app

COPY deps.edn .
RUN clj -A:build -P

COPY . .
RUN clj -T:build uber

FROM eclipse-temurin:22_36-jre-alpine AS runtime

WORKDIR /app

RUN apk add --no-cache \
    ca-certificates \
    tini \
    tzdata

COPY --from=builder /app/target/doplarr.jar .

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["java", "-jar", "/app/doplarr.jar"]

LABEL maintainer="Kiran Shila <me@kiranshila.com>"
LABEL org.opencontainers.image.source="https://github.com/DeitzFL/Doplarr"
