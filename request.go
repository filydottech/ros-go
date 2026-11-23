package routeros

import (
	"errors"
	"fmt"
	"strings"
)

// Attribute is one outbound =name=value word. Name must not be empty, start
// with a period, or contain an equals sign, and names must not repeat.
// Value may be empty, which sends a flag-like =name= word.
type Attribute struct {
	Name  string
	Value string
}

// Request is the payload Do and Stream accept. Attributes are sent in slice
// order, so the wire bytes do not depend on map iteration. Proplist becomes an
// optional .proplist=a,b word. Query contains preformed query words, such as
// those from Expr.Words; every raw entry must start with '?' and is sent
// unchanged, because RouterOS treats query order as significant.
type Request struct {
	Attributes []Attribute
	Proplist   []string
	Query      []string
}

// words validates the request and returns the sentence without a tag. The
// validator runs before anything is sent, so an invalid request never reaches
// the connection.
func (r Request) words(path string) ([]string, error) {
	if path == "" {
		return nil, errors.New("path is required")
	}

	words := make([]string, 0, 1+len(r.Attributes)+1+len(r.Query))
	words = append(words, path)
	seen := make(map[string]struct{}, len(r.Attributes))
	// Validation errors name the field and its index, never the caller's
	// value: a malformed attribute or query can contain a credential.
	for index, attribute := range r.Attributes {
		switch {
		case attribute.Name == "":
			return nil, fmt.Errorf("attribute %d has an empty name", index)
		case strings.HasPrefix(attribute.Name, "."):
			return nil, fmt.Errorf("attribute %d name starts with a period", index)
		case strings.Contains(attribute.Name, "="):
			return nil, fmt.Errorf("attribute %d name contains an equals sign", index)
		}
		if _, duplicate := seen[attribute.Name]; duplicate {
			return nil, fmt.Errorf("attribute %d repeats an earlier name", index)
		}
		seen[attribute.Name] = struct{}{}
		words = append(words, "="+attribute.Name+"="+attribute.Value)
	}

	if len(r.Proplist) > 0 {
		words = append(words, ".proplist="+strings.Join(r.Proplist, ","))
	}
	for index, query := range r.Query {
		if !strings.HasPrefix(query, "?") {
			return nil, fmt.Errorf("query %d must start with a question mark", index)
		}
		words = append(words, query)
	}
	for _, word := range words {
		if word == "" {
			return nil, errors.New("request contains an empty word")
		}
	}
	return words, nil
}
