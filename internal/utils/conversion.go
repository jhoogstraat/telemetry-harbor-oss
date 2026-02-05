package utils

import (
	"strconv"
)

// ToFloat64 safely converts various types (int, string, float32) to float64.
func ToFloat64(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}

	switch i := v.(type) {
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int:
		return float64(i), true
	case int32:
		return float64(i), true
	case int64:
		return float64(i), true
	case string:
		if f, err := strconv.ParseFloat(i, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}
