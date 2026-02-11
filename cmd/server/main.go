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
	url := "http://localhost:8080/api/progress"
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
	// In a real scenario, you'd send the total count in a metadata header or a 'StartSync' call.
	// For testing with your current setup, we'll use a placeholder or previous recipe length.
	totalExpected := 9 

	for {
		// receive the next chunk from the stream
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Ensure progress reaches 100% on completion
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
		

		

		// update PostgreSQL
		err = s.remoteDB.RegisterChunk(chunk.Hash, chunk.Data, len(chunk.Data))
		if err != nil {
			log.Printf("Error registering chunk in database: %v", err)
			return err
		}

		receivedCount++
		// Calculate and notify progress
		percent := (float64(receivedCount) / float64(totalExpected)) * 100
		notifyProgress("Uploading Chunks...", percent)

		fmt.Printf("Stored chunk: %s (%d bytes) - %.2f%%\n", chunk.Hash, len(chunk.Data), percent)
	}
}

func (s *server) DownloadFile(in *pb.FileRequest, stream pb.DeltaSync_DownloadFileServer) error {
	fmt.Printf("📂 Reconstruction request for file: %s\n", in.FileName)

	var hashes []string
	// 1. Get the list of hashes (the recipe) from Neon
	query := `SELECT chunk_hashes FROM file_recipes WHERE file_name = $1`
	
	// Use .Pool and context as required by pgx
	err := s.remoteDB.Pool.QueryRow(context.Background(), query, in.FileName).Scan(&hashes)
	if err != nil {
		log.Printf("❌ Error fetching recipe from Neon: %v", err)
		return err
	}

	for _, hash := range hashes {
		var chunkData []byte
		
		// 2. Fetch the actual binary data from the chunks table
		chunkQuery := `SELECT data FROM chunks WHERE hash = $1`
		err := s.remoteDB.Pool.QueryRow(context.Background(), chunkQuery, hash).Scan(&chunkData)
		if err != nil {
			log.Printf("❌ Error fetching chunk %s from Neon: %v", hash, err)
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
	// Initialize PostgreSQL connection
	remoteDB := db.InitPostgres()
	fmt.Println("Connected to PostgreSQL database successfully! (deltasync_db)")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterDeltaSyncServer(s, &server{remoteDB: remoteDB})
	reflection.Register(s)

	fmt.Println(" Delta-Sync server is active on port :50051")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}