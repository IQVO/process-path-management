package mcp

import (
	"slices"
	"testing"
)

// TestAuthorityMetaSetsPinnedKey pins the declaration vocabulary the
// static sensor and the runtime authority tests rely on (contracts G1/G2,
// ADR 0041): the helper sets exactly the fleet's authority key, and the
// four level constants are the strings L1..L4 — nothing else. Every tool
// here registers L1; L2/L3/L4 exist so a future tool declares its level
// with the same pinned shape instead of a string literal.
func TestAuthorityMetaSetsPinnedKey(t *testing.T) {
	levels := []string{authorityL1, authorityL2, authorityL3, authorityL4}
	if !slices.Equal(levels, []string{"L1", "L2", "L3", "L4"}) {
		t.Errorf("authority level constants = %v, want exactly L1, L2, L3, L4 (no L5)", levels)
	}
	for _, level := range levels {
		meta := authorityMeta(level)
		if got := meta[authorityMetaKey]; got != level {
			t.Errorf("authorityMeta(%q)[%q] = %v, want %q", level, authorityMetaKey, got, level)
		}
		if len(meta) != 1 {
			t.Errorf("authorityMeta(%q) = %v, want exactly one entry", level, meta)
		}
	}
}
