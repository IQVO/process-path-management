package main

import (
	"strings"
	"testing"
)

func TestLoadBootConfigRejects(t *testing.T) {
	tests := []struct {
		name         string
		analyticsURL string
		brokers      string
		wantNames    []string
	}{
		{"no ANALYTICS_DATABASE_URL", "", "kafka:9092", []string{"ANALYTICS_DATABASE_URL"}},
		{"no KAFKA_BROKERS", "postgres://u:p@h/db", "", []string{"KAFKA_BROKERS"}},
		{"blank KAFKA_BROKERS entries", "postgres://u:p@h/db", " , ", []string{"KAFKA_BROKERS"}},
		{"both missing are reported at once", "", "", []string{"ANALYTICS_DATABASE_URL", "KAFKA_BROKERS"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ANALYTICS_DATABASE_URL", tt.analyticsURL)
			t.Setenv("KAFKA_BROKERS", tt.brokers)
			_, err := loadBootConfig()
			if err == nil {
				t.Fatal("loadBootConfig() = nil error, want a boot error")
			}
			for _, name := range tt.wantNames {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q does not mention %q", err, name)
				}
			}
		})
	}
}

func TestLoadBootConfigAccepts(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "postgres://u:p@h/db")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	cfg, err := loadBootConfig()
	if err != nil {
		t.Fatalf("loadBootConfig() = %v, want nil", err)
	}
	if got := strings.Join(cfg.kafkaBrokers, "|"); got != "kafka-1:9092|kafka-2:9092" {
		t.Errorf("kafkaBrokers = %q", got)
	}
	if cfg.analyticsURL != "postgres://u:p@h/db" {
		t.Errorf("analyticsURL = %q", cfg.analyticsURL)
	}
}

func TestParseBrokers(t *testing.T) {
	if got := parseBrokers(""); got != nil {
		t.Errorf("parseBrokers(\"\") = %v, want nil (no localhost default)", got)
	}
}

// run() itself refuses to start without the mandatory environment, before it
// opens anything.
func TestRunFailsFastWithoutMandatoryEnvironment(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	err := run()
	if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") || !strings.Contains(err.Error(), "ANALYTICS_DATABASE_URL") {
		t.Fatalf("run() = %v, want a boot error naming both variables", err)
	}
}
