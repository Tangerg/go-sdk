// Copyright 2026 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"encoding/json"
	"testing"
)

func TestToolSchemaNumbersSurviveDecoding(t *testing.T) {
	const schema = `{"type":"object","properties":{"n":{"const":9007199254740993,"enum":[18446744073709551615,0.12345678901234567890123456789],"minimum":9007199254740993}},"required":["n"]}`
	var descriptor Tool
	if err := json.Unmarshal([]byte(`{"name":"exact","inputSchema":`+schema+`,"outputSchema":`+schema+`}`), &descriptor); err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{descriptor.InputSchema, descriptor.OutputSchema} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["properties"]) != `{"n":{"const":9007199254740993,"enum":[18446744073709551615,0.12345678901234567890123456789],"minimum":9007199254740993}}` {
			t.Fatalf("numeric constraints changed: %s", encoded)
		}
	}
}

func TestResourceContentPresence(t *testing.T) {
	for _, payload := range []string{`"text":""`, `"text":"body"`, `"blob":""`, `"blob":"AQ=="`} {
		t.Run(payload, func(t *testing.T) {
			wire := `{"uri":"file:///evidence",` + payload + `}`
			var value ResourceContents
			if err := json.Unmarshal([]byte(wire), &value); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != wire {
				t.Fatalf("resource changed: %s, want %s", encoded, wire)
			}
		})
	}
	for _, payload := range []string{``, `,"text":null`, `,"blob":null`, `,"text":3`, `,"blob":3`, `,"blob":[1]`, `,"text":"","blob":"AQ=="`, `,"text":"body","blob":""`} {
		var value ResourceContents
		if err := json.Unmarshal([]byte(`{"uri":"file:///evidence"`+payload+`}`), &value); err == nil {
			t.Fatalf("accepted malformed resource %s", payload)
		}
	}
	if _, err := json.Marshal(ResourceContents{URI: "file:///evidence", Text: "body", Blob: []byte{}}); err == nil {
		t.Fatal("encoded mixed resource")
	}
}
