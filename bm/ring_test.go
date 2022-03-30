package bm

import (
    //"fmt"
    "reflect"
    "strconv"
    "testing"
)

func TestRingOperations(t *testing.T) {
    var ring *Ring

    testSize := 10

    ring = NewRing(testSize)

    // Test Insert, Next and Prev function
    for i := 0; i < 10; i++ {
        nr := strconv.Itoa(i + 1)
        ring.Insert(i, "element "+nr)
    }
    for i := 0; i < 15; i++ {
        next := ring.Next().value

        if str, ok := next.(string); ok {
            if str != "element "+strconv.Itoa((i%testSize)+1) {
                t.Errorf("ring.go: Next or insert function not working properly")
            }
        }
    }

    ring.pointer = 0 //start from the back
    for i := ring.size - 1; i >= 0; i-- {
        prev := ring.Prev().value
        if str, ok := prev.(string); ok {
            if str != "element "+strconv.Itoa(i+1) {
                t.Errorf("ring.go: Previous or insert function not working properly")

            }
        }
    }

    AssertEqual(t, ring.Remove(2), true)
    AssertEqual(t, ring.IsFull(), false)

    AssertEqual(t, ring.HasKey(10), false)
    ring.Insert(10, "element 11")
    AssertEqual(t, ring.HasKey(10), true)
    AssertEqual(t, ring.IsFull(), true)

}

//https://gist.github.com/samalba/6059502
// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}) {
    if a == b {
        return
    }
    t.Errorf("Received %v (type %v), expected %v (type %v)", a, reflect.TypeOf(a), b, reflect.TypeOf(b))
}
