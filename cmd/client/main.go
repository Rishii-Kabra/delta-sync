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
	"google.golang.org/grpc/credentials"
)

func main() {
	// 1. Capture the file path and server address via command line flags
	filePath := flag.String("file", "", "The full path of the file you want to sync")
	// Updated default to your Render URL
	serverAddr := flag.String("server", "delta-sync-production.up.railway.app:443", "Server address")
	flag.Parse()

	if *filePath == "" {
		fmt.Println("❌ Usage error: You must specify a file to watch.")
		fmt.Println("Usage: go run cmd/client/main.go -file=\"your_file_path_here\"")
		os.Exit(1)
	}

	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		log.Fatalf("❌ Error: The file %s does not exist.", *filePath)
	}

	// 2. Setup fsnotify Watcher
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

	err = watcher.Add(*filePath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("👁️  Delta-Sync Watcher active on: %s\n", *filePath)
	fmt.Printf("🔗 Connecting to server: %s\n", *serverAddr)
	
	performSync(*filePath, *serverAddr)

	select {}
}

func performSync(filePath string, addr string) {
	localDB := db.InitSQLite("client_metadata.db")
	defer localDB.Conn.Close()

	chunks, err := chunker.AnalyzeFileVSC(filePath)
	if err != nil {
		log.Printf("Analysis failed: %v", err)
		return
	}

	var hashList []string
	for _, c := range chunks {
		hashList = append(hashList, c.Hash)
	}

	localDB.SaveFileIndex(filePath, strings.Join(hashList, ","))

	// 4. Establish SECURE gRPC Transport connection
	// Using system certs to allow connection to Render's HTTPS/TLS endpoint

	creds := credentials.NewClientTLSFromCert(nil, "") 
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Printf("Connection failed: %v", err)
		return
	}
	defer conn.Close()
	client := pb.NewDeltaSyncClient(conn)

	// Increased timeout to 30s to account for potential Render "Cold Start"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetMissingChunks(ctx, &pb.FileSignature{
		FileId:      filePath,
		ChunkHashes: hashList,
	})
	if err != nil {
		log.Printf("Sync failed: %v", err)
		return
	}

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

func downloadFromServer(targetFileName string, savePath string, addr string) {
	// Using secure credentials here as well
	creds := credentials.NewClientTLSFromCert(nil, "")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds))
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