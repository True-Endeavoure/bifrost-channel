// bifrost-channel — self-contained MCP-stdio + WS daemon for the Bifrost
// Claude Code plugin.
//
// One binary, three roles:
//
//	--mcp        MCP protocol over stdio (Claude Code .mcp.json target)
//	--monitor    Long-lived WS to bifrost-api.com, emits wake events on stdout
//	--stop-hook  Blocking Stop-hook handler, returns on wake or timeout
//	--version    Print version and exit
//
// Plugin runtime spawns the right mode at the right time.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var version = "dev"

func main() {
	mcpMode := flag.Bool("mcp", false, "MCP protocol over stdio")
	monitorMode := flag.Bool("monitor", false, "Background WS monitor; emits wake events on stdout")
	stopHookMode := flag.Bool("stop-hook", false, "Blocking stop-hook handler; returns on wake event")
	versionFlag := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println("bifrost-channel", version)
		return
	}

	apiKey := os.Getenv("BIFROST_API_KEY")
	bifrostURL := os.Getenv("BIFROST_URL")
	if bifrostURL == "" {
		bifrostURL = "https://bifrost-api.com"
	}
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] ERROR: BIFROST_API_KEY not set")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch {
	case *mcpMode:
		runMCP(ctx, apiKey, bifrostURL)
	case *monitorMode:
		runMonitor(ctx, apiKey, bifrostURL)
	case *stopHookMode:
		runStopHook(ctx, apiKey, bifrostURL)
	default:
		fmt.Fprintln(os.Stderr, "usage: bifrost-channel [--mcp | --monitor | --stop-hook | --version]")
		os.Exit(2)
	}
}

// ── MCP protocol over stdio ──────────────────────────────────────────

type jsonrpcReq struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResp struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any            `json:"result,omitempty"`
	Error   *jsonrpcError  `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func runMCP(ctx context.Context, apiKey, bifrostURL string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --mcp mode, version", version)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req jsonrpcReq
		if err := json.Unmarshal(line, &req); err != nil {
			writeMCPError(nil, -32700, "Parse error: "+err.Error())
			continue
		}
		handleMCP(req, apiKey, bifrostURL)
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] stdin scanner error:", err)
	}
}

func handleMCP(req jsonrpcReq, apiKey, bifrostURL string) {
	switch req.Method {
	case "initialize":
		writeMCPResult(req.ID, map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{"listChanged": false},
			},
			"serverInfo": map[string]any{
				"name":    "bifrost-channel",
				"version": version,
			},
		})
	case "notifications/initialized":
		// no response for notifications
	case "tools/list":
		writeMCPResult(req.ID, map[string]any{"tools": toolSpecs()})
	case "tools/call":
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		json.Unmarshal(req.Params, &params)
		handleToolCall(req.ID, params.Name, params.Arguments, apiKey, bifrostURL)
	default:
		writeMCPError(req.ID, -32601, "Method not found: "+req.Method)
	}
}

// toolSpec maps an MCP tool name onto a REST call.
type toolSpec struct {
	Name        string
	Description string
	Method      string // "GET" or "POST"
	Path        string // bifrost-api path, possibly with {placeholder}
	QueryKeys   []string // arg keys forwarded as ?query
	BodyMode    string   // "all" = whole args JSON, "" = none
	Properties  map[string]any
	Required    []string
}

var toolTable = []toolSpec{
	{Name: "debug_auth", Description: "Verify api_key + return scope/realm",
		Method: "GET", Path: "/auth/whoami",
		Properties: map[string]any{}},
	{Name: "ping", Description: "Health check — returns pong + version",
		Method: "", Path: "",
		Properties: map[string]any{}},
	{Name: "messages_send", Description: "Send a message to a channel or agent (e.g. 'zach', 'heimdall')",
		Method: "POST", Path: "/messages", BodyMode: "all",
		Properties: map[string]any{
			"channel": map[string]any{"type": "string", "description": "Channel ID or alias"},
			"content": map[string]any{"type": "string", "description": "Message content (markdown ok)"},
			"bot_id":  map[string]any{"type": "string", "description": "Optional bot to send as"},
		}, Required: []string{"channel", "content"}},
	{Name: "messages_list", Description: "List messages from a channel",
		Method: "GET", Path: "/messages", QueryKeys: []string{"channel", "limit", "since"},
		Properties: map[string]any{
			"channel": map[string]any{"type": "string"},
			"limit":   map[string]any{"type": "number"},
			"since":   map[string]any{"type": "string"},
		}, Required: []string{"channel"}},
	{Name: "messages_typing", Description: "Send typing indicator",
		Method: "POST", Path: "/typing", BodyMode: "all",
		Properties: map[string]any{"channel": map[string]any{"type": "string"}},
		Required: []string{"channel"}},
	{Name: "context_dashboard", Description: "Full context dashboard — messages, runes, health",
		Method: "GET", Path: "/context",
		Properties: map[string]any{}},
	{Name: "runes_list", Description: "List runes with optional filters",
		Method: "GET", Path: "/runes", QueryKeys: []string{"status", "priority", "owner", "type", "limit"},
		Properties: map[string]any{
			"status":   map[string]any{"type": "string"},
			"priority": map[string]any{"type": "number"},
			"owner":    map[string]any{"type": "string"},
			"limit":    map[string]any{"type": "number"},
		}},
	{Name: "runes_create", Description: "Create a new rune",
		Method: "POST", Path: "/runes", BodyMode: "all",
		Properties: map[string]any{
			"title": map[string]any{"type": "string"},
			"body":  map[string]any{"type": "string"},
		}, Required: []string{"title"}},
	{Name: "skills_search", Description: "Search skills by keyword",
		Method: "GET", Path: "/skills", QueryKeys: []string{"q", "limit", "namespace"},
		Properties: map[string]any{
			"q":         map[string]any{"type": "string"},
			"limit":     map[string]any{"type": "number"},
			"namespace": map[string]any{"type": "string"},
		}, Required: []string{"q"}},
	{Name: "skills_show", Description: "Read full skill content by ID or name",
		Method: "GET", Path: "/skills/{id}",
		Properties: map[string]any{"id": map[string]any{"type": "string"}},
		Required: []string{"id"}},
	{Name: "oracle_search", Description: "Search Oracle indexed docs",
		Method: "GET", Path: "/oracle/search", QueryKeys: []string{"q", "limit"},
		Properties: map[string]any{"q": map[string]any{"type": "string"}, "limit": map[string]any{"type": "number"}},
		Required: []string{"q"}},
	{Name: "knowledge_search", Description: "Search Bifrost knowledge graph (memory + AGE)",
		Method: "GET", Path: "/knowledge/search", QueryKeys: []string{"q", "limit"},
		Properties: map[string]any{"q": map[string]any{"type": "string"}, "limit": map[string]any{"type": "number"}},
		Required: []string{"q"}},
	{Name: "sql_execute", Description: "Execute SQL against Bifrost DB (read-only recommended)",
		Method: "POST", Path: "/admin/sql", BodyMode: "all",
		Properties: map[string]any{
			"query":    map[string]any{"type": "string"},
			"database": map[string]any{"type": "string", "enum": []string{"main", "oracle", "timescale"}},
		}, Required: []string{"query"}},
	{Name: "subscriptions_list", Description: "List channel subscriptions",
		Method: "GET", Path: "/subscriptions", QueryKeys: []string{"subscriber_id", "channel"},
		Properties: map[string]any{
			"subscriber_id": map[string]any{"type": "string"},
			"channel":       map[string]any{"type": "string"},
		}},
	{Name: "queue_check", Description: "Check work-queue for assigned items",
		Method: "GET", Path: "/queue/agent-check", QueryKeys: []string{"worker"},
		Properties: map[string]any{"worker": map[string]any{"type": "string"}}},
	{Name: "execute", Description: "Run JS via codemode.request() against bifrost-api. Async arrow fn returning result.",
		Method: "POST", Path: "/admin/codemode/exec", BodyMode: "all",
		Properties: map[string]any{"code": map[string]any{"type": "string"}},
		Required: []string{"code"}},
}

func toolSpecs() []map[string]any {
	out := make([]map[string]any, 0, len(toolTable))
	for _, t := range toolTable {
		entry := map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": t.Properties,
			},
		}
		if len(t.Required) > 0 {
			entry["inputSchema"].(map[string]any)["required"] = t.Required
		}
		out = append(out, entry)
	}
	return out
}

func findToolSpec(name string) *toolSpec {
	for i := range toolTable {
		if toolTable[i].Name == name {
			return &toolTable[i]
		}
	}
	return nil
}

func handleToolCall(id json.RawMessage, name string, args json.RawMessage, apiKey, bifrostURL string) {
	if name == "ping" {
		writeMCPResult(id, map[string]any{
			"content": []map[string]any{{"type": "text", "text": "pong from bifrost-channel " + version}},
		})
		return
	}

	spec := findToolSpec(name)
	if spec == nil {
		writeMCPError(id, -32601, "Unknown tool: "+name)
		return
	}

	var argMap map[string]any
	if len(args) > 0 {
		json.Unmarshal(args, &argMap)
	}

	// Substitute {placeholder} in path with arg value.
	path := spec.Path
	for k, v := range argMap {
		ph := "{" + k + "}"
		if strings.Contains(path, ph) {
			path = strings.Replace(path, ph, fmt.Sprintf("%v", v), -1)
			delete(argMap, k)
		}
	}

	// Build query string from QueryKeys.
	if len(spec.QueryKeys) > 0 && len(argMap) > 0 {
		q := url.Values{}
		for _, k := range spec.QueryKeys {
			if v, ok := argMap[k]; ok && v != nil {
				q.Set(k, fmt.Sprintf("%v", v))
			}
		}
		if len(q) > 0 {
			path = path + "?" + q.Encode()
		}
	}

	var body string
	var code int
	var err error
	if spec.Method == "GET" {
		body, code, err = bifrostGET(apiKey, bifrostURL, path)
	} else {
		body, code, err = bifrostPOST(apiKey, bifrostURL, path, args)
	}

	if err != nil {
		writeMCPError(id, -32000, err.Error())
		return
	}
	writeMCPResult(id, map[string]any{
		"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("HTTP %d\n%s", code, body)}},
	})
}

func writeMCPResult(id json.RawMessage, result any) {
	r := jsonrpcResp{JSONRPC: "2.0", ID: id, Result: result}
	b, _ := json.Marshal(r)
	fmt.Fprintln(os.Stdout, string(b))
}

func writeMCPError(id json.RawMessage, code int, msg string) {
	r := jsonrpcResp{JSONRPC: "2.0", ID: id, Error: &jsonrpcError{Code: code, Message: msg}}
	b, _ := json.Marshal(r)
	fmt.Fprintln(os.Stdout, string(b))
}

// ── HTTP helpers ─────────────────────────────────────────────────────

func bifrostGET(apiKey, baseURL, path string) (string, int, error) {
	req, err := http.NewRequest("GET", baseURL+path, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, nil
}

func bifrostPOST(apiKey, baseURL, path string, body json.RawMessage) (string, int, error) {
	req, err := http.NewRequest("POST", baseURL+path, strings.NewReader(string(body)))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, nil
}

// ── Monitor mode: WS → stdout JSON lines ──────────────────────────────

func runMonitor(ctx context.Context, apiKey, bifrostURL string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --monitor mode, version", version)

	wsURL := strings.Replace(bifrostURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1) + "/agent/stream/websocket"

	u, _ := url.Parse(wsURL)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+apiKey)

	c, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), header)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] WS dial failed:", err)
		os.Exit(1) // plugin runtime restarts us
	}
	defer c.Close()
	fmt.Fprintln(os.Stderr, "[bifrost-channel] WS connected:", wsURL)

	// Heartbeat ping every 30s
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, msg, err := c.ReadMessage()
		if err != nil {
			fmt.Fprintln(os.Stderr, "[bifrost-channel] WS read error:", err)
			os.Exit(1)
		}
		// Pass through as a single stdout line — plugin runtime delivers
		// each line to Claude as a notification.
		os.Stdout.Write(msg)
		os.Stdout.Write([]byte{'\n'})
	}
}

// ── Stop-hook mode: block until wake event ────────────────────────────

func runStopHook(ctx context.Context, apiKey, bifrostURL string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --stop-hook mode, version", version)

	// 5-minute ceiling (Claude Code's typical hook timeout)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	wsURL := strings.Replace(bifrostURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1) + "/agent/stream/websocket"

	header := http.Header{}
	header.Set("Authorization", "Bearer "+apiKey)

	c, _, err := websocket.DefaultDialer.DialContext(cctx, wsURL, header)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] WS dial failed:", err)
		return // exit 0 — empty return tells Claude to just stop
	}
	defer c.Close()

	type readResult struct {
		msg []byte
		err error
	}
	out := make(chan readResult, 1)
	go func() {
		_, m, e := c.ReadMessage()
		out <- readResult{m, e}
	}()

	select {
	case <-cctx.Done():
		// timeout — just exit, no wake
		return
	case r := <-out:
		if r.err != nil {
			fmt.Fprintln(os.Stderr, "[bifrost-channel] WS read error:", r.err)
			return
		}
		os.Stdout.Write(r.msg)
	}
}
