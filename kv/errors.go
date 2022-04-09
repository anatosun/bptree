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
