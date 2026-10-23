package service

import (
	"errors"
	"os"
	"path/filepath"

	"aiagent-gateway/internal/handoff"
)

// SelectArtifact is read-only. An absent baseline selects initial; corrupt
// metadata or a missing selected artifact never falls back to an older release.
func SelectArtifact(dataDir, initial, root string, uid uint32) (string, error) {
	d, err := handoff.ReadDecision(dataDir)
	artifact := initial
	if err == nil {
		artifact = filepath.Join(root, d.ColdStart, "gateway")
	} else if !errors.Is(err, handoff.ErrNoBaseline) && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err = ValidateArtifact(root, artifact, uid); err != nil {
		return "", err
	}
	return artifact, nil
}
