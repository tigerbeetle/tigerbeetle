package assert

import (
	"bytes"
	"reflect"
	"testing"
)

func isEmpty(obj interface{}) bool {
	if obj == nil {
		return true
	}

	value := reflect.ValueOf(obj)
	if value.IsNil() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Map, reflect.Slice:
		return value.Len() == 0
	default:
		return false
	}
}

func Empty(t *testing.T, obj interface{}) {
	if !isEmpty(obj) {
		t.Errorf("%v is not empty", obj)
	}
}

func getLength(obj interface{}) (ok bool, length int) {
	value := reflect.ValueOf(obj)
	defer func() {
		if e := recover(); e != nil {
			ok = false
		}
	}()
	return true, value.Len()
}

func Len(t *testing.T, obj interface{}, size int) {
	ok, length := getLength(obj)
	if !ok || length != size {
		t.Errorf("%v doesn't have size %v", obj, size)
	}
}

func isObjectEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	binaryA, ok := a.([]byte)
	if !ok {
		return reflect.DeepEqual(a, b)
	}

	binaryB, ok := b.([]byte)
	if !ok {
		return false
	}

	if binaryA == nil || binaryB == nil {
		return binaryA == nil && binaryB == nil
	}

	return bytes.Equal(binaryA, binaryB)
}

func isEqual(a, b interface{}) bool {
	if isObjectEqual(a, b) {
		return true
	}

	typeB := reflect.TypeOf(b)
	if typeB == nil {
		return false
	}

	valueA := reflect.ValueOf(a)
	if valueA.IsValid() && valueA.Type().ConvertibleTo(typeB) {
		return reflect.DeepEqual(valueA.Convert(typeB).Interface(), b)
	}

	return false
}

func Equal(t *testing.T, a, b interface{}) {
	if !isEqual(a, b) {
		t.Errorf("%v is not equal to %v", a, b)
	}
}

func NotEqual(t *testing.T, a, b interface{}) {
	if isEqual(a, b) {
		t.Errorf("%v is equal to %v", a, b)
	}
}

func isGreater(a, b interface{}) bool {
	a64, okA := a.(uint64)
	b64, okB := b.(uint64)
	return okA && okB && (a64 > b64)
}

func Greater(t *testing.T, a, b interface{}) {
	if !isGreater(a, b) {
		t.Errorf("%v is not greater than %v", a, b)
	}
}

func True(t *testing.T, condition bool) {
	if !condition {
		t.Errorf("condition is not true")
	}
}