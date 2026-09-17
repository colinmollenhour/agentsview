package sync

import (
	"container/list"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.kenn.io/agentsview/internal/parser"
)

const sourceFailureRetryInterval = 5 * time.Minute
const sourceFailureCacheLimit = 4096

type sourceFailure struct {
	key         verifiedSourceKey
	fingerprint parser.SourceFingerprint
	statDigest  uint64
	missingPath string
	err         error
	expires     time.Time
}

// sourceFailureCache bounds retained errors independently of archive size.
// Entries remain failures; they never authorize freshness or source deletion.
type sourceFailureCache struct {
	mu      sync.Mutex
	entries map[verifiedSourceKey]*list.Element
	order   list.List
}

func (c *sourceFailureCache) get(key verifiedSourceKey) (sourceFailure, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return sourceFailure{}, false
	}
	failure := element.Value.(sourceFailure)
	if !time.Now().Before(failure.expires) {
		delete(c.entries, key)
		c.order.Remove(element)
		return sourceFailure{}, false
	}
	c.order.MoveToBack(element)
	return failure, true
}

func (c *sourceFailureCache) remove(key verifiedSourceKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		delete(c.entries, key)
		c.order.Remove(element)
	}
}

func (c *sourceFailureCache) put(failure sourceFailure) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[verifiedSourceKey]*list.Element)
	}
	if element := c.entries[failure.key]; element != nil {
		element.Value = failure
		c.order.MoveToBack(element)
		return
	}
	if len(c.entries) == sourceFailureCacheLimit {
		oldest := c.order.Front()
		delete(c.entries, oldest.Value.(sourceFailure).key)
		c.order.Remove(oldest)
	}
	c.entries[failure.key] = c.order.PushBack(failure)
}

func (e *Engine) cachedProviderFailure(
	file parser.DiscoveredFile, source parser.SourceRef,
	statHash *pendingProviderStatHash, fingerprint *parser.SourceFingerprint,
) error {
	key := verifiedSourceKey{agent: file.Agent, path: source.Key}
	if e.forceParseRequested(file) || e.pathRewriter != nil || e.checkpointAudit.Load() {
		e.sourceFailures.remove(key)
		return nil
	}
	failure, ok := e.sourceFailures.get(key)
	if !ok {
		return nil
	}
	if failure.missingPath != "" {
		if _, err := os.Stat(failure.missingPath); errors.Is(err, os.ErrNotExist) {
			return failure.err
		}
	} else if failure.statDigest != 0 && statHash != nil && statHash.digest != 0 {
		if statHash.digest == failure.statDigest {
			return failure.err
		}
	} else if fingerprint == nil {
		return nil
	} else if *fingerprint == failure.fingerprint {
		return failure.err
	}
	e.sourceFailures.remove(key)
	return nil
}

// Only provider reads enter this cache. Cancellation and archive write errors
// must retry normally. Expiry also permits recovery from transient read errors
// that leave the provider's fingerprint unchanged.
func (e *Engine) cacheProviderFailure(
	file parser.DiscoveredFile, source parser.SourceRef,
	statHash *pendingProviderStatHash, fingerprint *parser.SourceFingerprint, err error,
) {
	if err == nil || e.pathRewriter != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	failure := sourceFailure{
		key: verifiedSourceKey{agent: file.Agent, path: source.Key},
		err: err, expires: time.Now().Add(sourceFailureRetryInterval),
	}
	if fingerprint != nil {
		if !deterministicProviderFailure(err) {
			return
		}
		failure.fingerprint = *fingerprint
		if statHash != nil {
			failure.statDigest = statHash.digest
		}
	} else {
		var pathErr *os.PathError
		if !errors.As(err, &pathErr) || !errors.Is(err, os.ErrNotExist) {
			return
		}
		failure.missingPath = pathErr.Path
	}
	e.sourceFailures.put(failure)
}

func deterministicProviderFailure(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var syntaxErr *jsontext.SyntacticError
	var semanticErr *json.SemanticError
	if errors.As(err, &syntaxErr) || errors.As(err, &semanticErr) {
		return true
	}
	var sqliteErr sqlite3.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrError
}
