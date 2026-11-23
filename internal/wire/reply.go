package wire

import (
	"fmt"
	"strings"
)

// Kind is the kind of a reply sentence.
type Kind int

const (
	// KindReply is a !re record.
	KindReply Kind = iota
	// KindDone is a !done terminator.
	KindDone
	// KindTrap is a !trap error.
	KindTrap
	// KindEmpty is a !empty acknowledgement.
	KindEmpty
	// KindFatal is a !fatal connection failure.
	KindFatal
)

func (k Kind) String() string {
	switch k {
	case KindReply:
		return "!re"
	case KindDone:
		return "!done"
	case KindTrap:
		return "!trap"
	case KindEmpty:
		return "!empty"
	case KindFatal:
		return "!fatal"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

var kindWords = map[string]Kind{
	"!re":    KindReply,
	"!done":  KindDone,
	"!trap":  KindTrap,
	"!empty": KindEmpty,
	"!fatal": KindFatal,
}

// Reply is one parsed reply sentence.
type Reply struct {
	Kind Kind
	// Tag is the routing tag, valid only when HasTag is true. RouterOS sends
	// an empty .tag= word, which the parser treats as untagged.
	Tag    string
	HasTag bool
	// Attributes holds =name=value words. A repeated name keeps its last
	// value; a value may itself contain an equals sign.
	Attributes map[string]string
	// APIAttributes holds .name=value words other than .tag.
	APIAttributes map[string]string
	// Plain holds words that are neither attributes nor API attributes; a
	// !fatal reason arrives this way.
	Plain []string
	// FatalReason is the joined Plain words of a !fatal reply.
	FatalReason string
}

// ParseReply parses one sentence. Only the five reply words are accepted; any
// other first word is a *ProtocolError.
func ParseReply(words []string) (Reply, error) {
	if len(words) == 0 {
		return Reply{}, &ProtocolError{Detail: "empty reply sentence"}
	}
	kind, ok := kindWords[words[0]]
	if !ok {
		return Reply{}, &ProtocolError{Detail: fmt.Sprintf("unknown reply word %q", words[0])}
	}

	reply := Reply{
		Kind:          kind,
		Attributes:    make(map[string]string),
		APIAttributes: make(map[string]string),
	}
	for _, word := range words[1:] {
		switch {
		case strings.HasPrefix(word, "="):
			name, value, err := splitAttribute(word[1:], "attribute")
			if err != nil {
				return Reply{}, err
			}
			reply.Attributes[name] = value
		case strings.HasPrefix(word, "."):
			name, value, err := splitAttribute(word[1:], "API attribute")
			if err != nil {
				return Reply{}, err
			}
			if name == "tag" {
				reply.Tag = value
				reply.HasTag = value != ""
				continue
			}
			reply.APIAttributes[name] = value
		default:
			reply.Plain = append(reply.Plain, word)
		}
	}
	if kind == KindFatal {
		reply.FatalReason = strings.Join(reply.Plain, " ")
	}
	return reply, nil
}

func splitAttribute(body, kind string) (name, value string, err error) {
	separator := strings.IndexByte(body, '=')
	if separator < 0 {
		if body == "" {
			return "", "", &ProtocolError{Detail: "empty " + kind + " key"}
		}
		return body, "", nil
	}
	name = body[:separator]
	if name == "" {
		return "", "", &ProtocolError{Detail: "empty " + kind + " key"}
	}
	return name, body[separator+1:], nil
}
