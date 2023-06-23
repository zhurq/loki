package encoding

// StringIter iterates over a sorted list of strings.
type StringIter interface {
	// Next advances the iterator and returns true if another value was found.
	Next() bool

	// At returns the value at the current iterator position.
	At() string

	// Err returns the last error of the iterator.
	Err() error
}
