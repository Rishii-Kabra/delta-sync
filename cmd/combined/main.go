package main

import (
	"delta-sync/delta-sync-pb/pkg/pb"
	"delta-sync/internal/db"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
)

type server struct {
    pb.UnimplementedDeltaSyncServer
    remoteDB *db.RemoteDB
}

func main() {
	remoteDB := db.InitPostgres() // Reads DATABASE_URL_DELTASYNC
	port := os.Getenv("PORT")
	if port == "" { port = "8080" }

	// 1. Initialize gRPC Server
	grpcServer := grpc.NewServer()
	pb.RegisterDeltaSyncServer(grpcServer, &server{remoteDB: remoteDB})

	// 2. Initialize Echo Dashboard
	e := echo.New()
	e.GET("/", func(c echo.Context) error { return c.File("web/index.html") })
	// ... (add your other dashboard routes here) ...

	// 3. Create a unified handler
	// This function checks if the request is gRPC; if not, it sends it to Echo
	mixedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			e.ServeHTTP(w, r)
		}
	})

	fmt.Printf("🚀 Unified Server starting on port %s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, mixedHandler))
}