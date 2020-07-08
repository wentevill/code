package adlist

//A generic doubly linked list implementation.
// similar C

// Value value
type Value interface {
	Value()
}

// Node node.
type Node struct {
	prev  *Node
	next  *Node
	value Value
}

// List list.
type List struct {
	head, tail *Node
	free       func(ptr Value)
	dup        func(ptr Value) Value
	match      func(ptr Value, key Value) int
	len        int64
}

// Directions for iterators
const (
	// iter of list head.
	ALStartHead = 0
	// iter of list tail.
	ALStartTail = 1
)

// Iter list iter
type Iter struct {
	next      *Node
	direction int
}

// Option opt.
type Option func(list *List)

// WithFree The 'free' is used to the value.
func WithFree(free func(ptr Value)) Option {
	return func(l *List) {
		l.free = free
	}
}

// WithDup The 'Dup' is used to copy the node value.
func WithDup(dup func(ptr Value) Value) Option {
	return func(l *List) {
		l.dup = dup
	}
}

// WithMatch matching a given key.
func WithMatch(match func(ptr Value, key Value) int) Option {
	return func(l *List) {
		l.match = match
	}
}

// ListCreate Create a new list.
// On error, nil is returned. Otherwise the pointer to the new list.
func ListCreate(opts ...Option) *List {
	list := &List{
		head: nil,
		tail: nil,

		free:  nil,
		dup:   nil,
		match: nil,

		len: 0,
	}

	for _, o := range opts {
		o(list)
	}

	return list
}

// Release Free the whole list.
// This function can't fail.
func (l *List) Release() {
	l.Empty()
	l.release()
}

func (l *List) release() {
	l = nil
}

// Empty Remove all the elements from the list without destroying the list itself.
func (l *List) Empty() {
	var len int64
	var current, next *Node

	current = l.head
	len = l.len
	for ; len > 0; len-- {
		next = current.next
		if l.free != nil {
			l.free(current.value)
		}
		current = next
	}
	l.head, l.tail = nil, nil
	l.len = 0
}

// AddNodeHead Add a new node to the list, to head, containing the
// specified 'value' pointer as value.
// On error, NULL is returned and no operation is performed (i.e. the
// list remains unaltered).
// On success the 'list' pointer you pass to the function is returned.
func (l *List) AddNodeHead(value Value) {
	node := &Node{
		value: value,
	}

	if l.head == nil {
		l.head, l.tail = node, node
		l.head.next, l.tail.prev = node, node
	} else {
		l.head.prev = node
		node.next = l.head

		l.head = l.head.prev
	}
	l.len++
}

// AddNodeTail Add a new node to the list, to tail, containing the
// specified 'value' pointer as value.
// On error, NULL is returned and no operation is performed (i.e. the
// list remains unaltered).
// On success the 'list' pointer you pass to the function is returned.
func (l *List) AddNodeTail(value Value) {
	node := &Node{
		value: value,
	}

	if l.head == nil {
		l.head, l.tail = node, node
		l.head.next, l.tail.prev = node, node
	} else {
		l.tail.next = node
		node.prev = l.tail

		l.tail = l.tail.next
	}
	l.len++
}

// InsertNode add new node to the list, after or before the old node, containing the
// specified 'value' pointer as value.
func (l *List) InsertNode(oldNode *Node, value Value, after int) {
	node := &Node{
		value: value,
	}

	if after > 0 {
		node.prev = oldNode
		node.next = oldNode.next
		if oldNode == l.tail {
			l.tail = node
		}
	} else {
		node.next = oldNode
		node.prev = oldNode.prev
		if oldNode == l.head {
			l.head = node
		}
	}

	if node.prev != nil {
		node.prev.next = node
	}
	if node.next != nil {
		node.next.prev = node
	}
	l.len++
}

// DelNode Remove the specified node from the specified list.
// It's up to the caller to free the private value of the node.
// This function can't fail.
func (l *List) DelNode(node *Node) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}

	if l.free != nil {
		l.free(node.value)
	}

	l.len--
}

// SearchKey Search the list for a node matching a given key.
// The match is performed using the 'match' method
// set with listSetMatchMethod(). If no 'match' method
// is set, the 'value' pointer of every node is directly
// compared with the 'key' pointer.
// On success the first matching node pointer is returned
// (search starts from head). If no matching node exists
// NULL is returned.
func (l *List) SearchKey(key Value) *Node {
	iter := l.Rewind()
	for node := iter.Next(); node != nil; node = iter.Next() {
		if l.match != nil {
			if l.match(node.value, key) > 0 {
				return node
			}
		} else {
			if key == node.value {
				return node
			}
		}
	}
	return nil
}

// GetIterator Returns a list iterator 'iter'. After the initialization every
// call to listNext() will return the next element of the list.
func (l *List) GetIterator(direction int) *Iter {
	iter := &Iter{
		direction: direction,
	}
	if direction == ALStartHead {
		iter.next = l.head
	} else {
		iter.next = l.tail
	}
	return iter
}

// Rewind Create an iterator in the list private iterator structure.
func (l *List) Rewind() *Iter {
	return &Iter{
		next:      l.head,
		direction: ALStartHead,
	}
}

// RewindTail Create an iterator in the list private iterator structure.
func (l *List) RewindTail() *Iter {
	return &Iter{
		next:      l.tail,
		direction: ALStartTail,
	}
}

// Next Return the next element of an iterator.
func (i *Iter) Next() *Node {
	current := i.next
	if current != nil {
		if i.direction == ALStartHead {
			i.next = current.next
		} else {
			i.next = current.prev
		}
	}
	return current
}

// Join  Add all the elements of the list 'o' at the end of the
// list 'l'. The list 'other' remains empty but otherwise valid.
func (l *List) Join(o *List) {
	if o.head != nil {
		o.head.prev = l.tail
	}

	if l.tail != nil {
		l.tail.next = o.head
	} else {
		l.head = o.head
	}
	if o.tail != nil {
		l.tail = o.tail
	}
	l.len += o.len

	o.head, o.tail = nil, nil
	o.len = 0
}

// Dup  Duplicate the whole list. On out of memory NULL is returned.
// On success a copy of the original list is returned.
func (l *List) Dup() *List {
	copy := &List{
		dup:   l.dup,
		free:  l.free,
		match: l.match,
	}

	iter := l.Rewind()
	for node := iter.Next(); node != nil; node = iter.Next() {
		var value Value
		if l.dup != nil {
			value = l.dup(node.value)
		} else {
			value = node.value
		}

		copy.AddNodeTail(value)
	}
	return copy
}
