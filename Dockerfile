FROM golang:1.25-alpine AS build
WORKDIR /src

COPY . .
RUN go build -o main .

FROM scratch
COPY --from=build /src/main /main

ENTRYPOINT [ "/main" ]
