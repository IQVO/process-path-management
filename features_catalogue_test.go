package main_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

// declaredCatalogueFile is the reviewed declaration of the paths every
// environment must define (ADR 0033); scripts/seed_catalogue.py applies it
// through POST /process-paths. Scenarios post the SAME bytes, so the file
// and the API can never disagree unnoticed.
const declaredCatalogueFile = "catalogue/paths.json"

// definesDeclaredCatalogueEntry posts the declared entry for pathId, as
// the seed script does, through the real router.
func (w2 *wave2) definesDeclaredCatalogueEntry(pathId string) error {
	raw, err := os.ReadFile(declaredCatalogueFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", declaredCatalogueFile, err)
	}
	var doc struct {
		Paths []map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("decode %s: %w", declaredCatalogueFile, err)
	}
	for _, entry := range doc.Paths {
		if entry["pathId"] != pathId {
			continue
		}
		body, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode declared %s: %w", pathId, err)
		}
		return w2.send(http.MethodPost, "/process-paths", string(body))
	}
	return fmt.Errorf("%s declares no entry %q", declaredCatalogueFile, pathId)
}
