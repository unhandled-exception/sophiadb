package utils

import (
	"fmt"
	"strings"
)

// JoinErrors объединяет ошибки из массива в строку с разделителем sep
func JoinErrors(errors []error, sep string) string {
	errorsStrings := make([]string, len(errors))
	for i, err := range errors {
		errorsStrings[i] = fmt.Sprintf("\"%s\"", err.Error())
	}

	return strings.Join(errorsStrings, sep)
}
