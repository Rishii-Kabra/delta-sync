package main

import (
	"bytes"
	"context"
	"delta-sync/delta-sync-pb/pkg/pb"
	"delta-sync/internal/db"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// server is used to implement the DeltaSync gRPC service
type server struct {
	pb.UnimplementedDeltaSyncServer
	remoteDB *db.RemoteDB
}

// notifyProgress sends an HTTP POST to the Web Server to update the WebSocket dashboard
func notifyProgress(fileName string, percent float64) {
	// Use the dynamic port assigned by Render for internal communication
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	url := fmt.Sprintf("http://localhost:%s/api/progress", port)
	
	data := map[string]interface{}{
		"file":    filepath.Base(fileName),
		"percent": percent,
	}
	jsonData, _ := json.Marshal(data)

	// Fire and forget to avoid blocking the gRPC stream
	go func() {
		_, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Failed to notify web dashboard: %v", err)
		}
	}()
}

// GetMissingChunks compares the client hashes against the server's state
func (s *server) GetMissingChunks(ctx context.Context, in *pb.FileSignature) (*pb.MissingChunksResponse, error) {
	fmt.Printf("Checking sync status for file: %s\n", in.FileId)

	// 1. Save the recipe so we know how to reconstruct the file later
	err := s.remoteDB.UpdateFileRecipe(in.FileId, in.ChunkHashes)
	if err != nil {
		log.Printf("Error saving recipe: %v", err)
	}

	missingHashes, err := s.remoteDB.GetMissingChunks(in.ChunkHashes)
	if err != nil {
		log.Printf("Database error: %v", err)
		return nil, err
	}

	fmt.Printf("Status: %d total chunks, %d missing from server. \n",
		len(in.ChunkHashes), len(missingHashes))

	return &pb.MissingChunksResponse{
		MissingHashes: missingHashes,
	}, nil
}

func (s *server) UploadChunks(stream pb.DeltaSync_UploadChunksServer) error {
	receivedCount := 0
	// Placeholder: In production, the client should send the total count first
	totalExpected := 1 

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			notifyProgress("Sync Complete", 100)
			return stream.SendAndClose(&pb.UploadStatus{
				Success: true,
				Message: "All chunks received successfully!",
			})
		}
		if err != nil {
			log.Printf("Error receiving chunk: %v", err)
			return err
		}

		// Register chunk in Neon using the bytes and length
		err = s.remoteDB.RegisterChunk(chunk.Hash, chunk.Data, len(chunk.Data))
		if err != nil {
			log.Printf("Error registering chunk in database: %v", err)
			return err
		}

		receivedCount++
		percent := (float64(receivedCount) / float64(totalExpected)) * 100
		notifyProgress("Uploading...", percent)

		fmt.Printf("Stored chunk: %s (%d bytes) - %.2f%%\n", chunk.Hash, len(chunk.Data), percent)
	}
}

func (s *server) DownloadFile(in *pb.FileRequest, stream pb.DeltaSync_DownloadFileServer) error {
	fmt.Printf("📂 Reconstruction request for file: %s\n", in.FileName)

	var hashes []string
	// 1. Get the recipe from Neon using the connection pool
	query := `SELECT chunk_hashes FROM file_recipes WHERE file_name = $1`
	err := s.remoteDB.Pool.QueryRow(context.Background(), query, in.FileName).Scan(&hashes)
	if err != nil {
		log.Printf("❌ Error fetching recipe from Neon: %v", err)
		return err
	}

	for _, hash := range hashes {
		var chunkData []byte
		
		// 2. Fetch binary data from the chunks table
		chunkQuery := `SELECT data FROM chunks WHERE hash = $1`
		err := s.remoteDB.Pool.QueryRow(context.Background(), chunkQuery, hash).Scan(&chunkData)
		if err != nil {
			log.Printf("❌ Error fetching chunk %s: %v", hash, err)
			return err
		}

		// 3. Stream the bytes back to the client
		err = stream.Send(&pb.ChunkPayload{
			Hash: hash,
			Data: chunkData,
			Size: int32(len(chunkData)),
		})
		if err != nil {
			return err
		}
	}

	fmt.Printf("✅ Successfully sent %d chunks for %s\n", len(hashes), in.FileName)
	return nil
}

func main() {
	// Initialize PostgreSQL connection (reads from DATABASE_URL_DELTASYNC)
	remoteDB := db.InitPostgres()
	fmt.Println("🚀 Success! Server is connected to Neon.")

	// Dynamically bind to the port assigned by Render
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Fallback for local testing
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	s := grpc.NewServer()
	pb.RegisterDeltaSyncServer(s, &server{remoteDB: remoteDB})
	reflection.Register(s)

	fmt.Printf("📡 Delta-Sync gRPC server active on port %s\n", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}