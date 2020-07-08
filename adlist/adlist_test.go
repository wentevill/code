package adlist

import (
	"fmt"
	"testing"
)

func TestListCreate(t *testing.T) {
	list := ListCreate()
	list.Release()
}

type valueT struct {
	value int
}

func (v *valueT) Value() {
	fmt.Println(v.value)
}

func free(ptr Value) {
	fmt.Printf("free: %d\n", ptr.(*valueT).value)
}

func dup(ptr Value) Value {
	return &valueT{
		value: ptr.(*valueT).value,
	}
}

func match(ptr Value, key Value) int {
	if key.(*valueT).value == ptr.(*valueT).value {
		return 1
	}
	return 0
}

func TestAdlist(t *testing.T) {
	tests := []struct {
		value int
		head  bool
	}{
		{
			value: 1,
			head:  true,
		},
		{
			value: 2,
			head:  true,
		},
		{
			value: 3,
			head:  true,
		},
		{
			value: 4,
			head:  false,
		},
	}

	list := ListCreate(
		WithDup(dup),
		WithFree(free),
		WithMatch(match),
	)

	for _, test := range tests {
		if test.head {
			list.AddNodeHead(&valueT{
				value: test.value,
			})
		} else {
			list.AddNodeTail(&valueT{
				value: test.value,
			})
		}
	}

	o := list.Dup()

	node := list.SearchKey(&valueT{
		value: tests[0].value,
	})
	if node.value.(*valueT).value != tests[0].value {
		t.FailNow()
	}

	list.InsertNode(node, &valueT{
		value: 5,
	}, 1)

	node2 := o.SearchKey(&valueT{
		value: tests[0].value,
	})

	o.DelNode(node2)

	list.Join(o)

	list.Release()
}
