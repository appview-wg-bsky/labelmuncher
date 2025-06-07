FROM denoland/deno:latest as builder
WORKDIR /app
COPY . .
RUN deno cache src/bin.ts

FROM denoland/deno:latest
WORKDIR /app
COPY --from=builder /app .
CMD ["deno", "run", "--allow-all", "src/bin.ts"]
