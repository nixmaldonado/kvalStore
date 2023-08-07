#Parent Image, "scratch" contains no distribution files.
#The resulting image and containers will have only the service binary
FROM golang:1.16 as build

#Copy the source files from the host
COPY . /src

#Set the working directory to the same place we copied the code
WORKDIR /src

#Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o kvs

FROM scratch

COPY --from=build /src/kvs .

EXPOSE 8080

CMD ["/kvs"]