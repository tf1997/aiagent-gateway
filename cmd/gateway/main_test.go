package main

import "testing"

func TestReadinessURL(t *testing.T) {
	for _, test := range []struct{ input, want string }{
		{":8080", "http://127.0.0.1:8080/readyz"},
		{"0.0.0.0:8080", "http://127.0.0.1:8080/readyz"},
		{"[::]:8080", "http://[::1]:8080/readyz"},
		{"127.0.0.1:9000", "http://127.0.0.1:9000/readyz"},
	} {
		got, err := readinessURL(test.input)
		if err != nil || got != test.want {
			t.Fatalf("%q: %q %v", test.input, got, err)
		}
	}
	if _, err := readinessURL("invalid"); err == nil {
		t.Fatal("invalid listener accepted")
	}
}
