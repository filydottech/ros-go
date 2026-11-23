package routeros

// Record holds the attributes of one reply. RouterOS values are wire strings;
// a number, a boolean, or a duration arrives as its textual form and is never
// coerced here.
type Record map[string]string

// Result is one completed command. Records preserves the arrival order of
// every !re reply, and Done holds the attributes of the terminating !done.
type Result struct {
	Records []Record
	Done    Record
}
