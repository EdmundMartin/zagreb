package expression

import (
	"fmt"
	"strconv"
	"strings"
)

// AttributeValue represents a DynamoDB attribute value.
type AttributeValue struct {
	S    *string                    `json:"S,omitempty"`
	N    *string                    `json:"N,omitempty"`
	B    []byte                     `json:"B,omitempty"`
	SS   []string                   `json:"SS,omitempty"`
	NS   []string                   `json:"NS,omitempty"`
	BS   [][]byte                   `json:"BS,omitempty"`
	M    map[string]*AttributeValue `json:"M,omitempty"`
	L    []*AttributeValue          `json:"L,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
}

// Update applies an update expression to an item.
func Update(item map[string]*AttributeValue, updateExpression string, expressionAttributeValues map[string]*AttributeValue) (map[string]*AttributeValue, error) {
	// Split the expression into clauses based on action keywords.
	// This is a simplified split and assumes actions are at the beginning of a clause.
	// A more robust parser would be needed for full DynamoDB compatibility.
	clauses := splitUpdateExpression(updateExpression)

	for _, clause := range clauses {
		parts := strings.Fields(clause)
		if len(parts) == 0 {
			continue
		}

		action := strings.ToUpper(parts[0])
		switch action {
		case "SET":
			if len(parts) < 4 || parts[2] != "=" {
				return nil, fmt.Errorf("invalid SET clause: %s", clause)
			}
			attrName := parts[1]
			attrValueStr := strings.Join(parts[3:], " ") // Handle values with spaces
			var attrValue *AttributeValue
			if strings.HasPrefix(attrValueStr, ":") {
				val, ok := expressionAttributeValues[attrValueStr]
				if !ok {
					return nil, fmt.Errorf("expression attribute value %s not found", attrValueStr)
				}
				attrValue = val
			} else {
				var err error
				attrValue, err = StringToAttributeValue(attrValueStr)
				if err != nil {
					return nil, fmt.Errorf("invalid value in SET clause: %s", err)
				}
			}
			item[attrName] = attrValue
		case "REMOVE":
			if len(parts) < 2 {
				return nil, fmt.Errorf("invalid REMOVE clause: %s", clause)
			}
			for i := 1; i < len(parts); i++ {
				attrName := parts[i]
				delete(item, attrName)
			}
		case "ADD":
			if len(parts) < 3 {
				return nil, fmt.Errorf("invalid ADD clause: %s", clause)
			}
			attrName := parts[1]
			addValueStr := strings.Join(parts[2:], " ")
			var addValue *AttributeValue
			if strings.HasPrefix(addValueStr, ":") {
				val, ok := expressionAttributeValues[addValueStr]
				if !ok {
					return nil, fmt.Errorf("expression attribute value %s not found", addValueStr)
				}
				addValue = val
			} else {
				var err error
				addValue, err = StringToAttributeValue(addValueStr)
				if err != nil {
					return nil, fmt.Errorf("invalid value in ADD clause: %s", err)
				}
			}

			existingValue, ok := item[attrName]
			if !ok || existingValue.N == nil {
				return nil, fmt.Errorf("attribute %s is not a number or does not exist for ADD operation", attrName)
			}

			existingNum, err := strconv.ParseFloat(*existingValue.N, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse existing number for ADD: %v", err)
			}
			addNum, err := strconv.ParseFloat(*addValue.N, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse add number for ADD: %v", err)
			}

			result := existingNum + addNum
			resultStr := strconv.FormatFloat(result, 'f', -1, 64)
			item[attrName] = &AttributeValue{N: &resultStr}

		case "DELETE":
			if len(parts) < 2 {
				return nil, fmt.Errorf("invalid DELETE clause: %s", clause)
			}
			for i := 1; i < len(parts); i++ {
				attrName := parts[i]
				// For scalar types, effectively delete by setting to nil
				// For set types (SS, NS, BS), a more complex parsing of values to delete would be needed.
				// This simplified implementation only removes the attribute entirely.
				delete(item, attrName)
			}
		default:
			return nil, fmt.Errorf("unsupported update action: %s", action)
		}
	}

	return item, nil
}

// splitUpdateExpression splits the update expression into individual action clauses.
// This is a very basic implementation and might not handle all edge cases of DynamoDB expressions.
func splitUpdateExpression(expression string) []string {
	var clauses []string
	currentClause := ""
	keywords := map[string]bool{
		"SET":    true,
		"REMOVE": true,
		"ADD":    true,
		"DELETE": true,
	}

	parts := strings.Fields(expression)
	for _, part := range parts {
		if keywords[strings.ToUpper(part)] && currentClause != "" {
			clauses = append(clauses, currentClause)
			currentClause = part
		} else {
			if currentClause == "" {
				currentClause = part
			} else {
				currentClause += " " + part
			}
		}
	}
	if currentClause != "" {
		clauses = append(clauses, currentClause)
	}
	return clauses
}

// StringToAttributeValue attempts to convert a string to an AttributeValue
// by inferring its type (S, N, BOOL).
func StringToAttributeValue(s string) (*AttributeValue, error) {
	// If the string is enclosed in double quotes, treat it as a string literal.
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") && len(s) > 1 {
		unquoted := s[1 : len(s)-1]
		return &AttributeValue{S: &unquoted}, nil
	}

	// Try to parse as boolean
	if b, err := strconv.ParseBool(s); err == nil {
		return &AttributeValue{BOOL: &b}, nil
	}

	// Try to parse as number
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		fs := strconv.FormatFloat(f, 'f', -1, 64)
		return &AttributeValue{N: &fs}, nil
	}

	// Otherwise, treat as string
	return &AttributeValue{S: &s}, nil
}

// GetAttributeValueType returns the type of the AttributeValue as a string (e.g., "S", "N", "BOOL").
func GetAttributeValueType(v *AttributeValue) string {
	if v == nil {
		return "NULL" // Or an empty string, depending on desired behavior for nil
	}
	if v.S != nil {
		return "S"
	}
	if v.N != nil {
		return "N"
	}
	if v.B != nil {
		return "B"
	}
	if v.SS != nil {
		return "SS"
	}
	if v.NS != nil {
		return "NS"
	}
	if v.BS != nil {
		return "BS"
	}
	if v.M != nil {
		return "M"
	}
	if v.L != nil {
		return "L"
	}
	if v.NULL != nil {
		return "NULL"
	}
	if v.BOOL != nil {
		return "BOOL"
	}
	return "" // Should not happen if AttributeValue is always valid
}
