package smolcache

// Element is an element of a linked list.
type Element struct {
	//CLOCK marker if this is recently used, must be first for atomic alignment
	used uint32
	//pad key out to 8 bytes
	_padding0 [8]byte
	// The value stored with this element.
	key   interface{}
	Value interface{}

	// Next and previous pointers in the singly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring
	next *Element

	// pad Elements out to be cache aligned
	_padding1 [1]byte
}

// Next returns the next list element or nil.
func (e *Element) Next() *Element {
	return e.next
}

// List represents a singly linked list.
type List struct {
	root Element // sentinel list element, only &root and root.next are used
	last *Element
}

// Init initializes or clears list l.
func (l *List) Init() *List {
	l.root.next = nil
	l.last = &l.root
	return l
}

// New returns an initialized list.
func New() *List { return new(List).Init() }

// Front returns the root element of list l.
func (l *List) Root() *Element {
	if l.last == nil {
		l.Init()
	}
	return &l.root
}

// insert inserts e after at, increments l.len, and returns e.
func (l *List) insert(e, at *Element) *Element {
	n := at.next
	at.next = e
	e.next = n
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *List) insertValue(k interface{}, v interface{}, at *Element) *Element {
	return l.insert(&Element{key: k, Value: v}, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *List) removeNext(e *Element) *Element {
	next := e.next
	e.next = e.next.next
	next.next = nil // avoid memory leaks
	return next
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *List) RemoveNext(e *Element) (key interface{}, val interface{}) {
	if e.next == nil {
		return
	}
	next := l.removeNext(e)
	if next == l.last {
		l.last = e
	}
	return next.key, next.Value
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *List) PushBack(k interface{}, v interface{}) *Element {
	if l.last == nil {
		l.Init()
	}
	l.last = l.insertValue(k, v, l.last)
	return l.last
}
