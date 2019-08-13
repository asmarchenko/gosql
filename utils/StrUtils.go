package utils

import (
	"strings"
	"encoding/hex"
	"strconv"
)

func needToEscape(str string) bool {
	return strings.Contains(str, ";") ||
		strings.Contains(str, "\"") ||
		strings.Contains(str, "'") ||
		strings.Contains(str, ";") ||
		strings.Contains(str, "\\") ||
		strings.Contains(str, "\n") ||
		strings.Contains(str, "\r")
}

func bytesToString(bytes []byte) string {
	return "x'" + hex.EncodeToString(bytes) + "'"
}

func PrepareStringValue(val interface{}) string {
	var strVal string
	switch val.(type) {
	case string:
		strVal = val.(string)
		if needToEscape(strVal) {
			strVal = bytesToString([]byte(strVal))
		} else {
			strVal = "'" + strVal + "'"
		}

	case []byte:
		strVal = bytesToString(val.([]byte))

	case int:
		strVal = strconv.FormatInt(int64(val.(int)), 10)

	case int64:
		strVal = strconv.FormatInt(val.(int64), 10)

	default:
		panic("Unexpected type of value. Supported types: string, []byte, int, int64")
	}
	return strVal
}

func ConcatValues(begin string, values []string, delim string, end string) string {
	return begin + strings.Join(values, delim) + end
}