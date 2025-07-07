package expression

import (
	"testing"
)

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func TestUpdate(t *testing.T) {
	// Test SET action with string
	t.Run("SET_string", func(t *testing.T) {
		item := map[string]*AttributeValue{
			"name": {S: stringPtr("old-name")},
		}
		updatedItem, err := Update(item, "SET name = :newname", map[string]*AttributeValue{":newname": {S: stringPtr("new-name")}})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if *updatedItem["name"].S != "new-name" {
			t.Errorf("expected name to be 'new-name', got '%s'", *updatedItem["name"].S)
		}
	})

	// Test SET action with number
	t.Run("SET_number", func(t *testing.T) {
		item := map[string]*AttributeValue{
			"age": {N: stringPtr("30")},
		}
		updatedItem, err := Update(item, "SET age = :newage", map[string]*AttributeValue{":newage": {N: stringPtr("40")}})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if *updatedItem["age"].N != "40" {
			t.Errorf("expected age to be '40', got '%s'", *updatedItem["age"].N)
		}
	})

	// Test SET action with boolean
	t.Run("SET_boolean", func(t *testing.T) {
		item := map[string]*AttributeValue{
			"isActive": {BOOL: boolPtr(true)},
		}
		updatedItem, err := Update(item, "SET isActive = :active", map[string]*AttributeValue{":active": {BOOL: boolPtr(false)}})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if *updatedItem["isActive"].BOOL != false {
			t.Errorf("expected isActive to be 'false', got '%t'", *updatedItem["isActive"].BOOL)
		}
	})

	// Test adding a new attribute with SET
	t.Run("SET_new_attribute", func(t *testing.T) {
		item := map[string]*AttributeValue{}
		updatedItem, err := Update(item, "SET city = :city", map[string]*AttributeValue{":city": {S: stringPtr("NewYork")}})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if *updatedItem["city"].S != "NewYork" {
			t.Errorf("expected city to be 'NewYork', got '%s'", *updatedItem["city"].S)
		}
	})

	// Test REMOVE action
	t.Run("REMOVE_attribute", func(t *testing.T) {
		item := map[string]*AttributeValue{
			"name": {S: stringPtr("old-name")},
			"age":  {N: stringPtr("30")},
		}
		updatedItem, err := Update(item, "REMOVE age", nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if _, ok := updatedItem["age"]; ok {
			t.Errorf("expected age to be removed, but it still exists")
		}
		if *updatedItem["name"].S != "old-name" {
			t.Errorf("expected name to be 'old-name', got '%s'", *updatedItem["name"].S)
		}
	})

	// Test combined SET and REMOVE
	t.Run("SET_and_REMOVE", func(t *testing.T) {
		item := map[string]*AttributeValue{
			"name":     {S: stringPtr("old-name")},
			"age":      {N: stringPtr("30")},
			"isActive": {BOOL: boolPtr(true)},
		}
		updatedItem, err := Update(item, "SET name = :newname REMOVE age", map[string]*AttributeValue{":newname": {S: stringPtr("new-name")}})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if *updatedItem["name"].S != "new-name" {
			t.Errorf("expected name to be 'new-name', got '%s'", *updatedItem["name"].S)
		}
		if _, ok := updatedItem["age"]; ok {
			t.Errorf("expected age to be removed, but it still exists")
		}
		if *updatedItem["isActive"].BOOL != true {
			t.Errorf("expected isActive to be 'true', got '%t'", *updatedItem["isActive"].BOOL)
		}
	})

	// Test invalid expression format
	t.Run("Invalid_expression", func(t *testing.T) {
		item := map[string]*AttributeValue{}
		_, err := Update(item, "INVALID expression", nil)
		if err == nil {
			t.Fatal("expected error, got no error")
		}
	})

	// Test invalid SET clause
	t.Run("Invalid_SET_clause", func(t *testing.T) {
		item := map[string]*AttributeValue{}
		_, err := Update(item, "SET name new-name", nil)
		if err == nil {
			t.Fatal("expected error, got no error")
		}
	})

	// Test invalid REMOVE clause
	t.Run("Invalid_REMOVE_clause", func(t *testing.T) {
		item := map[string]*AttributeValue{}
		_, err := Update(item, "REMOVE", nil)
		if err == nil {
			t.Fatal("expected error, got no error")
		}
	})
}

func TestStringToAttributeValue(t *testing.T) {
	// Test string conversion
	t.Run("string", func(t *testing.T) {
		val, err := StringToAttributeValue("hello")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val.S == nil || *val.S != "hello" {
			t.Errorf("expected string 'hello', got %v", val)
		}
	})

	// Test number conversion
	t.Run("number", func(t *testing.T) {
		val, err := StringToAttributeValue("123.45")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val.N == nil || *val.N != "123.45" {
			t.Errorf("expected number '123.45', got %v", val)
		}
	})

	// Test boolean true conversion
	t.Run("boolean_true", func(t *testing.T) {
		val, err := StringToAttributeValue("true")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val.BOOL == nil || *val.BOOL != true {
			t.Errorf("expected boolean 'true', got %v", val)
		}
	})

	// Test boolean false conversion
	t.Run("boolean_false", func(t *testing.T) {
		val, err := StringToAttributeValue("false")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val.BOOL == nil || *val.BOOL != false {
			t.Errorf("expected boolean 'false', got %v", val)
		}
	})
}

func TestGetAttributeValueType(t *testing.T) {
	// Test String type
	t.Run("String", func(t *testing.T) {
		val := &AttributeValue{S: stringPtr("test")}
		if got := GetAttributeValueType(val); got != "S" {
			t.Errorf("expected S, got %s", got)
		}
	})

	// Test Number type
	t.Run("Number", func(t *testing.T) {
		val := &AttributeValue{N: stringPtr("123")}
		if got := GetAttributeValueType(val); got != "N" {
			t.Errorf("expected N, got %s", got)
		}
	})

	// Test Binary type
	t.Run("Binary", func(t *testing.T) {
		val := &AttributeValue{B: []byte("binary")}
		if got := GetAttributeValueType(val); got != "B" {
			t.Errorf("expected B, got %s", got)
		}
	})

	// Test String Set type
	t.Run("StringSet", func(t *testing.T) {
		val := &AttributeValue{SS: []string{"a", "b"}}
		if got := GetAttributeValueType(val); got != "SS" {
			t.Errorf("expected SS, got %s", got)
		}
	})

	// Test Number Set type
	t.Run("NumberSet", func(t *testing.T) {
		val := &AttributeValue{NS: []string{"1", "2"}}
		if got := GetAttributeValueType(val); got != "NS" {
			t.Errorf("expected NS, got %s", got)
		}
	})

	// Test Binary Set type
	t.Run("BinarySet", func(t *testing.T) {
		val := &AttributeValue{BS: [][]byte{[]byte("x"), []byte("y")}}
		if got := GetAttributeValueType(val); got != "BS" {
			t.Errorf("expected BS, got %s", got)
		}
	})

	// Test Map type
	t.Run("Map", func(t *testing.T) {
		val := &AttributeValue{M: map[string]*AttributeValue{"key": {S: stringPtr("val")}}}
		if got := GetAttributeValueType(val); got != "M" {
			t.Errorf("expected M, got %s", got)
		}
	})

	// Test List type
	t.Run("List", func(t *testing.T) {
		val := &AttributeValue{L: []*AttributeValue{{S: stringPtr("item")}}}
		if got := GetAttributeValueType(val); got != "L" {
			t.Errorf("expected L, got %s", got)
		}
	})

	// Test Null type
	t.Run("Null", func(t *testing.T) {
		val := &AttributeValue{NULL: boolPtr(true)}
		if got := GetAttributeValueType(val); got != "NULL" {
			t.Errorf("expected NULL, got %s", got)
		}
	})

	// Test Boolean type
	t.Run("Boolean", func(t *testing.T) {
		val := &AttributeValue{BOOL: boolPtr(false)}
		if got := GetAttributeValueType(val); got != "BOOL" {
			t.Errorf("expected BOOL, got %s", got)
		}
	})

	// Test nil AttributeValue
	t.Run("NilAttributeValue", func(t *testing.T) {
		var val *AttributeValue = nil
		if got := GetAttributeValueType(val); got != "NULL" {
			t.Errorf("expected NULL for nil AttributeValue, got %s", got)
		}
	})

	// Test empty AttributeValue (no fields set)
	t.Run("EmptyAttributeValue", func(t *testing.T) {
		val := &AttributeValue{}
		if got := GetAttributeValueType(val); got != "" {
			t.Errorf("expected empty string for empty AttributeValue, got %s", got)
		}
	})
}
