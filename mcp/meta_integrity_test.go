// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	internaljson "github.com/Tangerg/go-sdk/internal/json"
)

func TestNativeMetadataRetainsJSONValues(t *testing.T) {
	const exact = `{"integer":9007199254740993,"uint":18446744073709551615,"fraction":0.12345678901234567890123456789,"negative":-9007199254740993,"null":null,"object":{},"array":[],"nested":[null,{"n":9007199254740993},[]]}`
	meta := `"_meta":{"exact":` + exact + `}`
	decodeExact := func(t *testing.T, raw []byte) any {
		t.Helper()
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	want := decodeExact(t, []byte(exact))
	for _, test := range []struct {
		name, wire string
		newValue   func() any
	}{
		{"request", `{"name":"probe",` + meta + `}`, func() any { return new(CallToolParamsRaw) }},
		{"result", `{"content":[],` + meta + `}`, func() any { return new(CallToolResult) }},
		{"tool", `{"name":"probe","inputSchema":{"type":"object"},` + meta + `}`, func() any { return new(Tool) }},
		{"prompt", `{"name":"probe",` + meta + `}`, func() any { return new(Prompt) }},
		{"resource", `{"name":"probe","uri":"file:///a",` + meta + `}`, func() any { return new(Resource) }},
		{"text", `{"type":"text","text":"hello",` + meta + `}`, func() any { return new(TextContent) }},
		{"image", `{"type":"image","data":"AQ==","mimeType":"image/png",` + meta + `}`, func() any { return new(ImageContent) }},
		{"audio", `{"type":"audio","data":"AQ==","mimeType":"audio/wav",` + meta + `}`, func() any { return new(AudioContent) }},
		{"link", `{"type":"resource_link","name":"probe","uri":"file:///a",` + meta + `}`, func() any { return new(ResourceLink) }},
		{"embedded", `{"type":"resource","resource":{"uri":"file:///a","text":"hello",` + meta + `},` + meta + `}`, func() any { return new(EmbeddedResource) }},
		{"resource contents", `{"uri":"file:///a","text":"hello",` + meta + `}`, func() any { return new(ResourceContents) }},
	} {
		for _, decoder := range []struct {
			name   string
			decode func([]byte, any) error
		}{
			{"standard", json.Unmarshal}, {"protocol", internaljson.Unmarshal},
		} {
			t.Run(test.name+"/"+decoder.name, func(t *testing.T) {
				value := test.newValue()
				if _, content := value.(Content); content {
					var result CallToolResult
					if err := decoder.decode([]byte(`{"content":[`+test.wire+`]}`), &result); err != nil {
						t.Fatal(err)
					}
					if len(result.Content) != 1 {
						t.Fatal("content missing")
					}
					value = result.Content[0]
				} else if err := decoder.decode([]byte(test.wire), value); err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var wire struct {
					Meta     map[string]json.RawMessage `json:"_meta"`
					Resource struct {
						Meta map[string]json.RawMessage `json:"_meta"`
					} `json:"resource"`
				}
				if err := json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decodeExact(t, wire.Meta["exact"]), want) {
					t.Fatalf("metadata changed: %s", encoded)
				}
				if test.name == "embedded" && !reflect.DeepEqual(decodeExact(t, wire.Resource.Meta["exact"]), want) {
					t.Fatalf("resource metadata changed: %s", encoded)
				}
			})
		}
	}
}

func TestMetaSetIsExactAndAtomic(t *testing.T) {
	var meta Meta
	if err := meta.Set("n", json.Number("9007199254740993")); err != nil {
		t.Fatal(err)
	}
	if string(meta["n"]) != "9007199254740993" {
		t.Fatalf("number = %s", meta["n"])
	}
	original := meta["n"]
	for _, value := range []any{json.RawMessage(`{"invalid":`), make(chan int)} {
		if err := meta.Set("n", value); err == nil {
			t.Fatal("invalid value accepted")
		}
		if !reflect.DeepEqual(meta["n"], original) {
			t.Fatal("failed encoding mutated metadata")
		}
		var empty Meta
		if err := empty.Set("n", value); err == nil || empty != nil {
			t.Fatalf("failed encoding initialized metadata: %v, %v", empty, err)
		}
	}
	params := &CallToolParams{}
	params.SetProgressToken(json.Number("9007199254740993"))
	if got := params.GetProgressToken(); got != json.Number("9007199254740993") {
		t.Fatalf("progress token = %v (%T)", got, got)
	}
}
