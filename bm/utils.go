package bm

import (
	"reflect"
	"testing"
)

// For testing purposes

//https://gist.github.com/samalba/6059502
// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		return
	}
	t.Errorf("Received %v (type %v), expected %v (type %v)", a, reflect.TypeOf(a), b, reflect.TypeOf(b))
}

func AssertNotEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		return
	}
	t.Errorf("Received %v (type %v) for both, expected different values", a, reflect.TypeOf(a))
}
