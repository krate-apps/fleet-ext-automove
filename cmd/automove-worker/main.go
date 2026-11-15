// Command automove-worker applies label/move rules on torrent events (public SDK only).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type rule struct {
	Label   string `json:"label"`
	Pattern string `json:"pattern"`
	Enabled bool   `json:"enabled"`
}

type ruleStore struct {
	Rules []rule `json:"rules"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
			reply(req.ID, map[string]any{"ok": true})
		case "fleet.shutdown":
			reply(req.ID, map[string]any{"ok": true})
			return
		case "ext.automove.list":
			st, err := loadRules()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, st)
		case "ext.automove.save":
			var st ruleStore
			if err := json.Unmarshal(req.Params, &st); err != nil {
				replyErr(req.ID, "invalid rules payload")
				continue
			}
			if err := saveRules(&st); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, st)
		case "ext.automove.add":
			var p rule
			if err := json.Unmarshal(req.Params, &p); err != nil || strings.TrimSpace(p.Pattern) == "" {
				replyErr(req.ID, "pattern required")
				continue
			}
			p.Enabled = true
			st, err := loadRules()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			st.Rules = append(st.Rules, p)
			if err := saveRules(st); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, st)
		default:
			replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
		}
	}
}

func loadRules() (*ruleStore, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "rules"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &ruleStore{Rules: []rule{}}, nil
	}
	var st ruleStore
	if err := json.Unmarshal(raw, &st); err != nil {
		return &ruleStore{Rules: []rule{}}, nil
	}
	return &st, nil
}

func saveRules(st *ruleStore) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "rules", "value": st})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	mu.Lock()
	id := nextHost
	nextHost++
	ch := make(chan hostReply, 1)
	pending[id] = ch
	mu.Unlock()
	req := map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id}
	line, _ := json.Marshal(req)
	line = append(line, '\n')
	outMu.Lock()
	_, err := os.Stdout.Write(line)
	outMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case rep := <-ch:
		if rep.err != "" {
			return nil, fmt.Errorf("%s", rep.err)
		}
		return rep.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func replyErr(id any, msg string) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
