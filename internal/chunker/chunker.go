package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/jotfs/fastcdc-go" // Ensure you run: go get github.com/jotfs/fastcdc-go
)

// Chunk represents a single variable-sized block of a file
type Chunk struct {
	Hash   string
	Size   int
	Offset int64
	Data   []byte // Stores raw bytes for reconstruction and gRPC transport
}

// Updated constants to align with high-performance CDC standards
const (
	MinChunkSize = 1024 * 32  // 32KB minimum
	AvgChunkSize = 1024 * 64  // 64KB target average
	MaxChunkSize = 1024 * 256 // 256KB maximum
)

// AnalyzeFileVSC performs Content-Defined Chunking using the FastCDC algorithm
func AnalyzeFileVSC(path string) ([]Chunk, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 1. Configure the FastCDC options for content-based splitting
	opts := fastcdc.Options{
		MinSize:     MinChunkSize,
		AverageSize: AvgChunkSize,
		MaxSize:     MaxChunkSize,
	}

	// 2. Initialize the Chunker with the file reader
	cdcChunker, err := fastcdc.NewChunker(file, opts)
	if err != nil {
		return nil, err
	}

	var chunks []Chunk
	var offset int64

	for {
		// 3. Next() finds the next boundary based on content fingerprints
		cdcData, err := cdcChunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 4. Fingerprinting: SHA-256 remains the standard for block identification
		hash := sha256.Sum256(cdcData.Data)

		chunks = append(chunks, Chunk{
			Hash:   hex.EncodeToString(hash[:]),
			Size:   len(cdcData.Data),
			Offset: offset,
			Data:   cdcData.Data, // Bytes are preserved for gRPC transport
		})

		// Update offset based on the actual size of the content-defined chunk
		offset += int64(len(cdcData.Data))
	}

	return chunks, nil
}