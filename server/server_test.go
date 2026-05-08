package server

import (
	"testing"
)

func TestValidateSetArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "valid_set_3_args",
			args: []string{"set", "foo", "bar"},
			want: true,
		},
		{
			name: "valid_set_4_args",
			args: []string{"set", "foo", "bar", "60"},
			want: true,
		},
		{
			name: "invalid_set_missing_key_value",
			args: []string{"set"},
			want: false,
		},
		{
			name: "invalid_set_missing_value",
			args: []string{"set", "foo"},
			want: false,
		},
		{
			name: "invalid_set_too_many_args",
			args: []string{"set", "foo", "bar", "60", "extra"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateSetArgs(tt.args)
			if got != tt.want {
				t.Fatalf("validateSetArgs(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestValidateArity(t *testing.T) {
	tests := []struct {
		name  string
		arity int
		args  []string
		want  bool
	}{
		{
			name:  "exact_arity_valid",
			arity: 2,
			args:  []string{"get", "key"},
			want:  true,
		},
		{
			name:  "exact_arity_too_many",
			arity: 2,
			args:  []string{"get", "key", "extra"},
			want:  false,
		},
		{
			name:  "minimum_arity_valid",
			arity: -3,
			args:  []string{"set", "key", "value"},
			want:  true,
		},
		{
			name:  "minimum_arity_too_few",
			arity: -3,
			args:  []string{"set", "key"},
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateArity(tt.arity, tt.args)
			if got != tt.want {
				t.Fatalf("validateArity(%d, %v) = %v, want %v", tt.arity, tt.args, got, tt.want)
			}
		})
	}
}
