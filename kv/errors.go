package kv

import "fmt"

type KeyNotFoundError struct {
	Value interface{}
}

func (err *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key %v not found", err.Value)
}

type InsertionError struct {
	Type     interface{}
	Value    interface{}
	Size     interface{}
	Position int
	Capacity int
}

func (err *InsertionError) Error() string {
	return fmt.Sprintf("could not insert %v with value %v at position %d in slice of size %d/%d", err.Type, err.Value, err.Position, err.Size, err.Capacity)
}

type DeletionError struct {
	Type     interface{}
	Value    interface{}
	Size     interface{}
	Position int
	Capacity int
}

func (err *DeletionError) Error() string {
	return fmt.Sprintf("could not delete %v with value %v at position %d in slice of size %d/%d", err.Type, err.Value, err.Position, err.Size, err.Capacity)
}

type OverflowError struct {
	Type   interface{}
	Max    interface{}
	Actual interface{}
}

func (err *OverflowError) Error() string {
	return fmt.Sprintf("the size of the slice \"%v\" exceeds its supposed bound %v/%v", err.Type, err.Max, err.Actual)
}

type IllegalValueError struct {
	Value interface{}
	Type  interface{}
}

func (err *IllegalValueError) Error() string {
	return fmt.Sprintf("illegal value %v cannot be used as type %v", err.Value, err.Type)
}

type BufferOverflowError struct {
	Max    interface{}
	Cursor interface{}
}

func (err *BufferOverflowError) Error() string {
	return fmt.Sprintf("buffer overflow: max %v, cursor %v", err.Max, err.Cursor)
}

type InvalidSizeError struct {
	Got    interface{}
	Should interface{}
}

func (err *InvalidSizeError) Error() string {
	return fmt.Sprintf("invalid size for data, got %v expected %v", err.Got, err.Should)
}
