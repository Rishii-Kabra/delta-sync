package main

import (
	"context"
	"delta-sync/delta-sync-pb/pkg/pb"
	"delta-sync/internal/db"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"os"

	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/gorilla/websocket"
)

var (
    upgrader = websocket.Upgrader{
        CheckOrigin: func(r *http.Request) bool { return true },
    }
    // Simple way to track connected dashboard clients
    clients = make(map[*websocket.Conn]bool)
)

func main() {
	// 1. Initialize PostgreSQL connection for the Web Dashboard
	// Using your defined credentials: admin123 on deltasync_db
	remoteDB := db.InitPostgres()
	fmt.Println("🌐 Web Dashboard connected to PostgreSQL (deltasync_db)")

	e := echo.New()

	// 2. Serve the static HTML file
	e.GET("/", func(c echo.Context) error {
		return c.File("web/index.html")
	})

	// 3. Updated API for HTMX injection
	e.GET("/api/files", func(c echo.Context) error {
    files, err := remoteDB.GetAllRecipes()
    if err != nil {
        return c.String(http.StatusInternalServerError, "Failed to load registry")
    }

    html := ""
    for _, f := range files {
        displayName := filepath.Base(f.Name)
        // Format time to show a nice timestamp
        timeLabel := f.UpdatedAt.Format("Jan 02, 15:04")

        html += fmt.Sprintf(`
            <div class="group flex items-center justify-between p-6 rounded-2xl bg-white/[0.02] border border-white/5 hover:border-green-500/40 hover:bg-green-500/[0.03] transition-all duration-500">
                <div class="flex items-center gap-5">
                    <div class="w-12 h-12 rounded-xl bg-slate-800/50 flex items-center justify-center group-hover:bg-green-500/10 transition-all border border-white/5 group-hover:border-green-500/20">
                        <svg class="w-6 h-6 text-slate-500 group-hover:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"></path>
                        </svg>
                    </div>
                    <div class="overflow-hidden">
                        <p class="text-sm font-bold text-slate-200 truncate max-w-[200px] sm:max-w-md tracking-tight">%s</p>
                        <div class="flex items-center gap-2 mt-0.5">
                            <p class="text-[10px] text-slate-500 uppercase tracking-[0.1em] font-medium">Verified Block</p>
                            <span class="text-slate-700">•</span>
                            <p class="text-[10px] text-green-500/60 mono font-bold uppercase tracking-tighter">Synced: %s</p>
                        </div>
                    </div>
                </div>
                <div class="flex items-center gap-4">
                    <a href="/download?file=%s" class="opacity-0 translate-x-4 group-hover:opacity-100 group-hover:translate-x-0 transition-all duration-300 bg-green-500 text-slate-950 text-[10px] font-black px-5 py-2.5 rounded-xl hover:bg-green-400 active:scale-90 shadow-[0_0_15px_rgba(74,222,128,0.2)]">
                        RECONSTRUCT
                    </a>
                </div>
            </div>`, displayName, timeLabel, f.Name)
    }

    if html == "" {
        html = `<div class="text-center py-20 text-slate-600 text-xs tracking-widest uppercase italic">Registry Empty.</div>`
    }

    return c.HTML(http.StatusOK, html)
})

	// 4. Download Route: Bridges HTTP to gRPC
	e.GET("/download", func(c echo.Context) error {
		fileName := c.QueryParam("file")
		
		// Establish gRPC connection to the server
		conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return c.String(http.StatusInternalServerError, "Could not connect to Delta-Sync gRPC server")
		}
		defer conn.Close()
		client := pb.NewDeltaSyncClient(conn)

		// Request the reconstruction stream
		stream, err := client.DownloadFile(context.Background(), &pb.FileRequest{FileName: fileName})
		if err != nil {
			return c.String(http.StatusNotFound, "File recipe not found in PostgreSQL")
		}

		// Set headers for browser-side file reconstruction
		downloadName := filepath.Base(fileName)
		c.Response().Header().Set(echo.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%s", downloadName))
		c.Response().Header().Set(echo.HeaderContentType, "application/octet-stream")
		c.Response().WriteHeader(http.StatusOK)

		// Stream bytes from gRPC server to the browser
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			c.Response().Write(chunk.Data)
		}
		return nil
	})

	// WebSocket endpoint for the dashboard
    e.GET("/ws", func(c echo.Context) error {
        ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
        if err != nil {
            return err
        }
        defer ws.Close()
        clients[ws] = true
        
        // Keep connection alive
        for {
            if _, _, err := ws.ReadMessage(); err != nil {
                delete(clients, ws)
                break
            }
        }
        return nil
    })

    // Internal endpoint for the gRPC server to post progress updates
    e.POST("/api/progress", func(c echo.Context) error {
        var data map[string]interface{}
        if err := c.Bind(&data); err != nil {
            return err
        }

        // Broadcast progress HTML to all connected dashboard clients via HTMX WebSocket extension
        progressHTML := fmt.Sprintf(`
            <div id="sync-progress" hx-swap-oob="true" class="mb-4 p-4 bg-blue-900 border border-blue-700 rounded-md">
                <p class="text-xs font-bold text-blue-300 mb-1">SYNCING: %s</p>
                <div class="w-full bg-gray-700 rounded-full h-2">
                    <div class="bg-blue-500 h-2 rounded-full transition-all" style="width: %v%%"></div>
                </div>
            </div>`, data["file"], data["percent"])

        for client := range clients {
            client.WriteMessage(websocket.TextMessage, []byte(progressHTML))
        }
        return c.NoContent(http.StatusOK)
    })

	// 5. Start the Web Server
	port := os.Getenv("PORT")
	if port == "" {
    	port = "8080"
	}
	e.Logger.Fatal(e.Start(":" + port))
}