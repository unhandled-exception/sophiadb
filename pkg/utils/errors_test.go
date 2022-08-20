package utils_test

import (
	"os"
	"testing"

	"github.com/unhandled-exception/sophiadb/pkg/utils"
)

func TestJoinErrors(t *testing.T) {
	type args struct {
		errors []error
		sep    string
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty errors list",
			args: args{
				errors: []error{},
				sep:    ", ",
			},
			want: "",
		},
		{
			name: "not empty errors list",
			args: args{
				errors: []error{
					os.ErrClosed,
					os.ErrExist,
				},
				sep: ", ",
			},
			want: `"file already closed", "file already exists"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.JoinErrors(tt.args.errors, tt.args.sep); got != tt.want {
				t.Errorf("joinErrors() = %v, want %v", got, tt.want)
			}
		})
	}
}
