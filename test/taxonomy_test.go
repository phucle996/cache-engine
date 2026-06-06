package cacheEngine_test

import (
	"fmt"
	"testing"

	cacheEngine_taxonomy "github.com/phucle996/cache-engine/taxonomy"
)

// TestTaxonomyError covers the error message formatting logic.
func TestTaxonomyError(t *testing.T) {
	err1 := cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "msg", nil)
	if err1.Error() != "[UNREGISTERED_KEY] msg" {
		t.Errorf("unexpected error string: %s", err1.Error())
	}

	err2 := cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "msg", fmt.Errorf("underlying"))
	if err2.Error() != "[L2_FAILED] msg: underlying" {
		t.Errorf("unexpected error string: %s", err2.Error())
	}
}
