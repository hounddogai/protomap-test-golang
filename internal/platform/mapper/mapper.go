// Package mapper copies fields between values by name, for the adapters that translate between API versions.
package mapper

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CopyFields copies the named exported fields of the struct src points to into the struct dst points to.
func CopyFields(dst, src any, names ...string) error {
	from := reflect.ValueOf(src)
	to := reflect.ValueOf(dst)
	if from.Kind() != reflect.Pointer || to.Kind() != reflect.Pointer {
		return fmt.Errorf("mapper: CopyFields needs pointers, got %T and %T", dst, src)
	}
	from, to = from.Elem(), to.Elem()
	for _, name := range names {
		value := from.FieldByName(name)
		field := to.FieldByName(name)
		if !value.IsValid() || !field.IsValid() || !field.CanSet() {
			return fmt.Errorf("mapper: no field %s to copy", name)
		}
		field.Set(value)
	}
	return nil
}

// CopyProtoField copies the field `from` of src into the field `to` of dst, by their protobuf names, when both
// hold the same kind of value.
func CopyProtoField(dst, src proto.Message, from, to string) error {
	source := src.ProtoReflect()
	target := dst.ProtoReflect()
	sourceField := source.Descriptor().Fields().ByName(protoreflect.Name(from))
	targetField := target.Descriptor().Fields().ByName(protoreflect.Name(to))
	if sourceField == nil || targetField == nil {
		return fmt.Errorf("mapper: %s has no field %s or %s has no field %s",
			source.Descriptor().FullName(), from, target.Descriptor().FullName(), to)
	}
	if sourceField.Kind() != targetField.Kind() {
		return fmt.Errorf("mapper: %s and %s differ in kind", from, to)
	}
	target.Set(targetField, source.Get(sourceField))
	return nil
}
