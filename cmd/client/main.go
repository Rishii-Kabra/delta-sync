package main

import (
	"context"
	"delta-sync/delta-sync-pb/pkg/pb"
	"delta-sync/internal/chunker"
	"delta-sync/internal/db"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Capture the file path via command line argument
	// Example: go run cmd/client/main.go -file="C:\path\to\your\file.txt"
	filePath := flag.String("file", "", "The full path of the file you want to sync")
	serverAddr := flag.String("server", "localhost:50051", "Server address (e.g. shaggy-pets-run.loca.lt)")
	flag.Parse()

	// Validate that a path was provided
	if *filePath == "" {
		fmt.Println("❌ Usage error: You must specify a file to watch.")
		fmt.Println("Usage: go run cmd/client/main.go -file=\"your_file_path_here\"")
		os.Exit(1)
	}

	// Verify the file exists on the local system
	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		log.Fatalf("❌ Error: The file %s does not exist.", *filePath)
	}

	// 2. Setup fsnotify Watcher for real-time monitoring
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	// 3. Background monitoring loop
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// Trigger sync only on Write events per technical contract
				if event.Op&fsnotify.Write == fsnotify.Write {
					fmt.Printf("📝 Changes detected in: %s. Initiating Delta-Sync...\n", event.Name)
					performSync(*filePath, *serverAddr)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("watcher error:", err)
			}
		}
	}()

	// Add the user-specified file to the watcher
	err = watcher.Add(*filePath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("👁️  Delta-Sync Watcher active on: %s\n", *filePath)
	fmt.Printf("🔗 Connecting to server: %s\n", *serverAddr)
	
	// Perform initial sync on startup to catch up with server state
	performSync(*filePath, *serverAddr)

	// Block main goroutine to keep watcher alive
	select {}
}

// performSync handles the chunking, fingerprinting, and gRPC upload
func performSync(filePath string, addr string) {
	// Initialize local SQLite for indexing as per contract
	localDB := db.InitSQLite("client_metadata.db")
	defer localDB.Conn.Close()

	// 1. Chunking and Hashing (SHA-256)
	chunks, err := chunker.AnalyzeFileVSC(filePath)
	if err != nil {
		log.Printf("Analysis failed: %v", err)
		return
	}

	var hashList []string
	for _, c := range chunks {
		hashList = append(hashList, c.Hash)
	}

	// 2. Update local SQLite index
	localDB.SaveFileIndex(filePath, strings.Join(hashList, ","))

	// 3. Establish gRPC Transport connection
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Connection failed: %v", err)
		return
	}
	defer conn.Close()
	client := pb.NewDeltaSyncClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 4. The "Diff": Identify missing chunks on server
	resp, err := client.GetMissingChunks(ctx, &pb.FileSignature{
		FileId:      filePath,
		ChunkHashes: hashList,
	})
	if err != nil {
		log.Printf("Sync failed: %v", err)
		return
	}

	// 5. Reconstruction: Stream only missing bytes to server
	if len(resp.MissingHashes) > 0 {
		fmt.Printf("📤 Syncing %d new/modified chunks...\n", len(resp.MissingHashes))
		stream, err := client.UploadChunks(context.Background())
		if err != nil {
			log.Printf("Stream failed: %v", err)
			return
		}

		for _, missingHash := range resp.MissingHashes {
			for _, c := range chunks {
				if c.Hash == missingHash {
					stream.Send(&pb.ChunkPayload{Hash: c.Hash, Data: c.Data})
				}
			}
		}
		stream.CloseAndRecv()
		fmt.Println("✅ Delta-Sync Complete!")
	} else {
		fmt.Println("✨ Server is already synchronized with this version.")
	}
}

// downloadFromServer provides manual reconstruction logic
func downloadFromServer(targetFileName string, savePath string) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Connection failed: %v", err)
		return
	}
	defer conn.Close()
	client := pb.NewDeltaSyncClient(conn)
	
	stream, err := client.DownloadFile(context.Background(), &pb.FileRequest{FileName: targetFileName})
	if err != nil {
		log.Fatal(err)
	}

	file, _ := os.Create(savePath)
	defer file.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		file.Write(chunk.Data)
	}
	fmt.Println("🎉 File reconstructed locally!")
}