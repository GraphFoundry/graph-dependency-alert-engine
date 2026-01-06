package graphservice

import (
	"encoding/json"
	"testing"
)

func TestIntOrObject_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "valid integer",
			input:    `{"podCount": 5}`,
			expected: 5,
		},
		{
			name:     "zero",
			input:    `{"podCount": 0}`,
			expected: 0,
		},
		{
			name:     "null",
			input:    `{"podCount": null}`,
			expected: 0,
		},
		{
			name:     "empty object",
			input:    `{"podCount": {}}`,
			expected: 0,
		},
		{
			name:     "object with properties",
			input:    `{"podCount": {"running": 3, "pending": 1}}`,
			expected: 0,
		},
		{
			name:     "string (malformed)",
			input:    `{"podCount": "5"}`,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result struct {
				PodCount intOrObject `json:"podCount"`
			}
			err := json.Unmarshal([]byte(tt.input), &result)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if int(result.PodCount) != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, int(result.PodCount))
			}
		})
	}
}

func TestServiceDTO_UnmarshalWithIntOrObject(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedPods int
		shouldError  bool
	}{
		{
			name:         "normal service response",
			input:        `{"name": "frontend", "namespace": "default", "podCount": 3, "availability": 0.99}`,
			expectedPods: 3,
			shouldError:  false,
		},
		{
			name:         "podCount as object",
			input:        `{"name": "frontend", "namespace": "default", "podCount": {}, "availability": 0.99}`,
			expectedPods: 0,
			shouldError:  false,
		},
		{
			name:         "podCount as null",
			input:        `{"name": "frontend", "namespace": "default", "podCount": null, "availability": 0.99}`,
			expectedPods: 0,
			shouldError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var svc serviceDTO
			err := json.Unmarshal([]byte(tt.input), &svc)
			if (err != nil) != tt.shouldError {
				t.Errorf("expected error: %v, got: %v", tt.shouldError, err)
			}
			if !tt.shouldError && int(svc.PodCount) != tt.expectedPods {
				t.Errorf("expected podCount %d, got %d", tt.expectedPods, int(svc.PodCount))
			}
		})
	}
}
