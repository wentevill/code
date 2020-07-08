package dict

import (
	"errors"
	"math"
)

// Error
var (

	// ErrDict dict error
	ErrDict = errors.New("add entity to dict err")
)

const (
	// DictOK ok
	DictOK int = 1
	// DictErr fail.
	DictErr int = 2
)

const (
	/* Using dictEnableResize() / dictDisableResize() we make possible to
	 * enable/disable resizing of the hash table as needed. This is very important
	 * for Redis, as we use copy-on-write and don't want to move too much memory
	 * around when there is a child performing saving operations.
	 *
	 * Note that even when dict_can_resize is set to 0, not all resizes are
	 * prevented: a hash table is still allowed to grow if the ratio between
	 * the number of elements and the buckets > dict_force_resize_ratio. */
	dictCanResize        int    = 1
	dictForceResizeRatio uint64 = 5

	// This is the initial size of every hash table
	dictHTInitailSize uint64 = 4
)

// Key dict key
type Key interface {
	HashFunction() uint64
	Compare(Key) int
	Dup() Key
	Destructor()
}

// Value dict value
type Value interface {
	Dup() Value
	Destructor()
}

// Entry dict entry
type Entry struct {
	key Key
	// Value value
	// TODO int,float need not use the pointer
	// Refer to the union of C
	value Value

	next *Entry
}

// dictht This is our hash table structure. Every dictionary has two of this as we
// implement incremental rehashing, for the old to the new table.
type dictht struct {
	table []*Entry

	size     uint64
	sizemask uint64
	used     uint64
}

// Dict dict.
type Dict struct {
	ht [2]*dictht

	// rehashing not in progress if rehashidx == -1
	rehashidx int64

	// number of iterators currently running
	iterators uint64
}

// Create a new hash tables
func Create() *Dict {
	dict := &Dict{
		rehashidx: -1,
		iterators: 0,
	}

	_dictInit(dict)
	return dict
}

func _dictInit(dict *Dict) {
	dict.ht[0] = _dictReset()
	dict.ht[1] = _dictReset()
}

//  Reset a hash tabl
func _dictReset() *dictht {
	return &dictht{
		table:    nil,
		size:     0,
		sizemask: 0,
		used:     0,
	}
}

// Add an element to the target hash table
func (d *Dict) Add(key Key, value Value) error {
	entry := d.addRaw(key, nil)
	if entry == nil {
		return ErrDict
	}

	entry.setVal(value)
	return nil
}

// Replace Add or Overwrite:
// Add an element, discarding the old value if the key already exists.
// Return 1 if the key was added from scratch, 0 if there was already an
// element with such key and Replace() just performed a value update
// operation.
func (d *Dict) Replace(key Key, value Value) int {
	var entry, existing *Entry
	var auxentry Entry

	/* Try to add the element. If the key
	 * does not exists dictAdd will succeed. */
	entry = d.addRaw(key, &existing)
	if entry != nil {
		entry.setVal(value)
		return 1
	}
	/* Set the new value and free the old one. Note that it is important
	 * to do that in this order, as the value may just be exactly the same
	 * as the previous one. In this context, think to reference counting,
	 * you want to increment (set), and then decrement (free), and not the
	 * reverse. */
	auxentry = *existing
	existing.setVal(value)
	auxentry.freeVal()
	return 0
}

// Low level add or find:
// This function adds the entry but instead of setting a value returns the
// dictEntry structure to the user, that will make sure to fill the value
// field as he wishes.
// This function is also directly exposed to the user API to be called
// mainly in order to store non-pointers inside the hash value.
// If key already exists NULL is returned, and "*existing" is populated
// with the existing entry if existing is not NULL.
// If key was added, the hash entry is returned to be manipulated by the caller.
func (d *Dict) addRaw(key Key, existing **Entry) *Entry {

	var (
		index int64
	)

	if d.isRehashing() {
		d.rehashStep()
	}

	// Get the index of the new element, or -1 if
	// the element already exists.
	if index = d.keyIndex(key, key.HashFunction(), existing); index == -1 {
		return nil
	}

	ht := d.ht[0]
	if d.isRehashing() {
		ht = d.ht[1]
	}

	entiry := &Entry{}
	entiry.next = ht.table[index]
	ht.table[index] = entiry
	ht.used++

	// Set the hash entry fields.
	entiry.setKey(key)
	return entiry
}

func (e *Entry) setKey(key Key) {
	e.key = key.Dup()
}

func (e *Entry) setVal(val Value) {
	e.value = val.Dup()
}

func (e *Entry) freeKey() {
	e.key.Destructor()
}

func (e *Entry) freeVal() {
	e.value.Destructor()
}

func (d *Dict) keyIndex(key Key, hash uint64, existing **Entry) int64 {
	var (
		idx uint64
		he  *Entry
	)

	if existing != nil {
		*existing = nil
	}

	/* Expand the hash table if needed */
	if d.expandIfNeeded() == DictErr {
		return -1
	}

	for table := 0; table <= 1; table++ {
		idx = hash & d.ht[table].sizemask

		// Search if this slot does not already contain the given key
		he = d.ht[table].table[idx]
		for ; he != nil; he = he.next {
			if key == he.key || key.Compare(he.key) == 0 {
				if existing != nil {
					*existing = he
				}
				return -1
			}
		}
		if !d.isRehashing() {
			break
		}
	}

	return int64(idx)
}

// Find find the entry by the key
// if not fond will return nil
func (d *Dict) Find(key Key) *Entry {
	var he *Entry
	var h, idx, table uint64

	//  dict is empty
	if d.size() == 0 {
		return nil
	}

	if d.isRehashing() {
		d.rehashStep()
	}

	h = key.HashFunction()

	for table = 0; table <= 1; table++ {
		idx = h & d.ht[table].sizemask
		he = d.ht[table].table[idx]
		for ; he != nil; he = he.next {
			if he.key == key || key.Compare(he.key) == 0 {
				return he
			}
		}
		if !d.isRehashing() {
			return nil
		}
	}

	return nil
}

// FetchValue fetch vale of the key
func (d *Dict) FetchValue(key Key) Value {
	he := d.Find(key)
	if he == nil {
		return nil
	}
	return he.value
}

// Delete Remove an element, returning DictOK on success or DictErr if the
// element was not found.
func (d *Dict) Delete(key Key) int {
	if he := d.genericDelete(key, 0); he == nil {
		return DictErr
	}
	return DictOK
}

// Unlink Remove an element from the table, but without actually releasing
// the key, value and dictionary entry. The dictionary entry is returned
// if the element was found (and unlinked from the table), and the user
// should later call `dictFreeUnlinkedEntry()` with it in order to release it.
// Otherwise if the key is not found, NULL is returned.
// This function is useful when we want to remove something from the hash
// table but want to use its value before actually deleting the entry.
func (d *Dict) Unlink(key Key) *Entry {
	return d.genericDelete(key, 1)
}

// Resize the table to the minimal size that contains all the elements,
// but with the invariant of a USED/BUCKETS ratio near to <= 1
func (d *Dict) Resize() int {
	if d.isRehashing() {
		return DictErr
	}

	minimal := d.ht[0].used
	if minimal < dictHTInitailSize {
		minimal = dictHTInitailSize
	}

	return d.expand(minimal)
}

// Close Clear & Release the hash table
func (d *Dict) Close() {
	d.clear(d.ht[0])
	d.clear(d.ht[1])
	d = nil
}

// Search and remove an element. This is an helper function for
// Delete() and Unlink(), please check the top comment
// of those functions.
func (d *Dict) genericDelete(key Key, nofree int) *Entry {
	var h, idx uint64
	var he, prevHe *Entry
	var table int

	//  dict is empty
	if d.size() == 0 {
		return nil
	}

	if d.isRehashing() {
		d.rehashStep()
	}

	h = key.HashFunction()

	for table = 0; table <= 1; table++ {
		idx = h & d.ht[table].sizemask
		he = d.ht[table].table[idx]

		prevHe = nil

		for ; he != nil; he = he.next {
			if he.key == key || key.Compare(he.key) == 0 {
				/* Unlink the element from the list */
				if prevHe != nil {
					prevHe.next = he.next
				} else {
					d.ht[table].table[idx] = he.next
				}

				if nofree == 0 {
					he.freeKey()
					he.freeVal()
				}
				d.ht[table].used--
				return he
			}
			prevHe = he
		}
		if !d.isRehashing() {
			return nil
		}
	}
	return nil
}

func (d *Dict) size() uint64 {
	return d.ht[0].size + d.ht[1].size
}

func (d *Dict) isRehashing() bool {
	return d.rehashidx != -1
}

// This function performs just a step of rehashing, and only if there are
// no safe iterators bound to our hash table. When we have iterators in the
// middle of a rehashing we can't mess with the two hash tables otherwise
// some element can be missed or duplicated.
// This function is called by common lookup or update operations in the
// dictionary so that the hash table automatically migrates from H1 to H2
// while it is actively used.
func (d *Dict) rehashStep() {
	if d.iterators == 0 {
		d.rehash(1)
	}
}

// Performs N steps of incremental rehashing. Returns 1 if there are still
// keys to move from the old to the new hash table, otherwise 0 is returned.
// Note that a rehashing step consists in moving a bucket (that may have more
// than one key as we use chaining) from the old to the new hash table, however
// since part of the hash table may be composed of empty spaces, it is not
// guaranteed that this function will rehash even a single bucket, since it
// will visit at max N*10 empty buckets in total, otherwise the amount of
// work it does would be unbound and the function may block for a long time.
func (d *Dict) rehash(n int) int {
	// Max number of empty buckets to visit
	emptyVisits := n * 10
	if !d.isRehashing() {
		return 0
	}

	for ; n > 0 && d.ht[0].used != 0; n-- {
		var (
			de     *Entry
			nextde *Entry
		)

		/* Note that rehashidx can't overflow as we are sure there are more
		 * elements because ht[0].used != 0 */

		for d.ht[0].table[d.rehashidx] == nil {
			d.rehashidx++
			if emptyVisits == 0 {
				return 1
			}
			emptyVisits--
		}

		de = d.ht[0].table[d.rehashidx]
		for de != nil {
			var h uint64

			nextde = de.next
			/* Get the index in the new hash table */
			h = de.key.HashFunction() & d.ht[1].sizemask
			de.next = d.ht[1].table[h]
			d.ht[1].table[h] = de
			d.ht[0].used--
			d.ht[1].used++
			de = nextde
		}
		d.ht[0].table[d.rehashidx] = nil
		d.rehashidx++
	}

	// Check if we already rehashed the whole table.
	if d.ht[0].used == 0 {
		d.ht[0].table = nil
		d.ht[0] = d.ht[1]
		d.ht[1] = _dictReset()
		d.rehashidx = -1
	}

	return 1
}

// Expand the hash table if needed
func (d *Dict) expandIfNeeded() int {
	// Incremental rehashing already in progress.
	if d.isRehashing() {
		return DictOK
	}

	// If the hash table is empty expand it to the initial size.
	if d.ht[0].size == 0 {
		return d.expand(dictHTInitailSize)
	}

	if d.ht[0].used >= d.ht[0].size &&
		(dictCanResize == 1 || d.ht[0].used/d.ht[0].size > dictForceResizeRatio) {
		return d.expand(d.ht[0].used * 2)
	}

	return DictOK
}

// Expand or create the hash table
func (d *Dict) expand(size uint64) int {
	/* the size is invalid if it is smaller than the number of
	 * elements already inside the hash table */
	if d.isRehashing() || d.ht[0].used > size {
		return DictErr
	}

	realsize := nextPower(size)

	/*Rehashing to the same table size is not useful.*/
	if realsize == d.ht[0].size {
		return DictErr
	}

	/* Allocate the new hash table and initialize all pointers to NULL*/
	n := &dictht{
		size:     realsize,
		sizemask: realsize - 1,
		used:     0,

		table: make([]*Entry, realsize),
	}

	/* Is this the first initialization? If so it's not really a rehashing
	 * we just set the first hash table so that it can accept keys. */
	if d.ht[0].table == nil {
		d.ht[0] = n
		return DictOK
	}

	/*Prepare a second hash table for incremental rehashing */
	d.ht[1] = n
	d.rehashidx = 0

	return DictOK
}

// Our hash table capability is a power of two
func nextPower(size uint64) uint64 {
	if int64(size>>1) >= math.MaxInt64 {
		return uint64(math.MaxInt64) + 1
	}

	i := dictHTInitailSize
	for i < size {
		i *= 2
	}

	return i
}

// Destroy an entire dictionary
func (d *Dict) clear(ht *dictht) int {
	var i uint64
	var he, nextHe *Entry
	for i = 0; i < ht.size && ht.used > 0; i++ {
		for he = ht.table[i]; he != nil; he = nextHe {
			nextHe = he.next

			he.freeKey()
			he.freeVal()

			ht.used--
		}
	}

	ht.table = nil

	ht = _dictReset()
	return DictOK
}
