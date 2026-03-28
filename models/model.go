package models

// Model represents a persistable entity with common lifecycle operations.
// All models that implement this interface provide consistent methods for
// creation, reading, updating, and message generation.
// The Create method accepts variadic interface{} parameters for flexibility,
// allowing each implementation to define what source data it needs.
type Model interface {
	Create(source ...interface{}) error
	Read() error
	Update() error
	Message() error
}
