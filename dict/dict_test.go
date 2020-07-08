package dict

import (
	"fmt"
	"testing"
)

type keyT struct {
	key uint64
}

func (k *keyT) HashFunction() uint64 {
	return k.key % 10
}

func (k *keyT) Compare(key Key) int {
	if k.key == key.(*keyT).key {
		return 0
	}
	return 1
}

func (k *keyT) Dup() Key {
	return &keyT{
		key: k.key,
	}
}

func (k *keyT) Destructor() {
	fmt.Printf("Destructor key: %d\n", k.key)
}

type valueT struct {
	value uint64
}

func (v *valueT) Dup() Value {
	return &valueT{
		value: v.value,
	}
}
func (v *valueT) Destructor() {
	fmt.Printf("Destructor value: %d\n", v.value)
}

func TestDict(t *testing.T) {

	var num = 20

	tests := make([]struct {
		key   *keyT
		value *valueT
	}, num)

	for i := 0; i < num; i++ {
		tests[i].key = &keyT{
			key: uint64(i),
		}
		tests[i].value = &valueT{
			value: uint64(i),
		}
	}

	dict := Create()
	defer dict.Close()

	for _, test := range tests {
		dict.Add(test.key, test.value)
	}

	value := dict.FetchValue(tests[3].key)
	if value == nil {
		t.FailNow()
	}

	if value.(*valueT).value != tests[3].value.value {
		t.FailNow()
	}

	if DictOK != dict.Delete(tests[2].key) {
		t.FailNow()
	}

	if entry := dict.Find(tests[2].key); entry != nil {
		t.FailNow()
	}

	entry := dict.Unlink(tests[4].key)
	if entry == nil {
		t.FailNow()
	}

	if tests[4].key.Compare(entry.key) != 0 {
		t.FailNow()
	}

	if 1 != dict.Replace(&keyT{key: uint64(num)}, &valueT{value: uint64(num)}) {
		t.FailNow()
	}

	if 0 != dict.Replace(tests[5].key, &valueT{value: 55555}) {
		t.FailNow()
	}

	value = dict.FetchValue(tests[5].key)
	if value == nil {
		t.FailNow()
	}

	if value.(*valueT).value != 55555 {
		t.FailNow()
	}

}
