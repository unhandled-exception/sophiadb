package storage

import (
	"fmt"
	"strings"
)

func joinErrors(errors []error, sep string) string {
	errorsStrings := make([]string, len(errors))
	for i, err := range errors {
		errorsStrings[i] = fmt.Sprintf("\"%s\"", err.Error())
	}
	return strings.Join(errorsStrings, sep)
}
