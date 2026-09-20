package sync

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

type failingReadProvider struct {
	*processFixtureProvider
	parseErr error
	attempts int
}

func (p *failingReadProvider) Parse(ctx context.Context, req parser.ParseRequest) (parser.ParseOutcome, error) {
	p.attempts++
	if p.parseErr != nil {
		return parser.ParseOutcome{}, p.parseErr
	}
	return p.processFixtureProvider.Parse(ctx, req)
}

type failingReadFactory struct{ provider *failingReadProvider }

func (f failingReadFactory) Definition() parser.AgentDef                       { return f.provider.Definition() }
func (f failingReadFactory) Capabilities() parser.Capabilities                 { return f.provider.Capabilities() }
func (f failingReadFactory) NewProvider(parser.ProviderConfig) parser.Provider { return f.provider }

func TestProviderFailureRetriesOnChangeForceOrExpiry(t *testing.T) {
	for _, recovery := range []string{
		"fingerprint", "force", "expiry", "schema", "syntax",
		"cancellation", "locked", "transient", "permission",
	} {
		t.Run(recovery, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				path := filepath.Join(root, "failed.jsonl")
				require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
				source := parser.SourceRef{
					Provider: parser.AgentCowork, Key: path, DisplayPath: path, FingerprintKey: path,
				}
				fingerprint := parser.SourceFingerprint{Key: path, Size: 7, MTimeNS: 42}
				provider := &failingReadProvider{
					processFixtureProvider: newProcessFixtureProvider(source, fingerprint, parser.ParseOutcome{
						SkipReason: parser.SkipNonInteractive, ResultSetComplete: true,
					}),
					parseErr: io.ErrUnexpectedEOF,
				}
				cacheable := true
				switch recovery {
				case "schema":
					provider.parseErr = fmt.Errorf("query provider schema: %w", sqlite3.Error{Code: sqlite3.ErrError})
				case "syntax":
					var value any
					provider.parseErr = json.Unmarshal([]byte(`{"unfinished":`), &value)
					require.Error(t, provider.parseErr)
				case "cancellation":
					provider.parseErr = context.Canceled
					cacheable = false
				case "locked":
					provider.parseErr = fmt.Errorf("read provider store: %w", sqlite3.Error{Code: sqlite3.ErrBusy})
					cacheable = false
				case "transient":
					provider.parseErr = errors.New("temporary provider read failure")
					cacheable = false
				case "permission":
					provider.parseErr = &os.PathError{Op: "open", Path: path, Err: os.ErrPermission}
					cacheable = false
				}
				engine := NewEngine(openTestDB(t), EngineConfig{
					AgentDirs: map[parser.AgentType][]string{parser.AgentCowork: {root}},
					Machine:   "local", ProviderFactories: []parser.ProviderFactory{failingReadFactory{provider}},
					ProviderMigrationModes: map[parser.AgentType]parser.ProviderMigrationMode{
						parser.AgentCowork: parser.ProviderMigrationProviderAuthoritative,
					},
				})
				t.Cleanup(engine.Close)
				plan := ChangedPathPlan{Files: []parser.DiscoveredFile{{
					Path: path, Agent: parser.AgentCowork, ProviderSource: &source, ProviderProcess: true,
				}}}
				for range 3 {
					result, err := engine.SyncChangedPathPlanContext(t.Context(), plan, nil)
					require.Error(t, err)
					assert.Equal(t, 1, result.Stats.Failed)
					assert.Zero(t, result.Stats.Skipped, "a cached error must not become a successful skip")
				}
				if !cacheable {
					assert.Equal(t, 3, provider.attempts, "transient errors must not suppress another attempt")
				} else {
					assert.Equal(t, 1, provider.attempts, "unchanged failures must not invoke the parser repeatedly")
				}
				switch recovery {
				case "fingerprint", "schema", "syntax":
					provider.fingerprint.Hash = "repaired"
				case "force":
					plan.Files[0].ForceParse = true
				case "expiry":
					time.Sleep(5 * time.Minute)
				}
				before := provider.attempts
				provider.parseErr = nil
				result, err := engine.SyncChangedPathPlanContext(t.Context(), plan, nil)
				require.NoError(t, err)
				assert.Zero(t, result.Stats.Failed)
				assert.Equal(t, before+1, provider.attempts)
			})
		})
	}
}

type failureFingerprintFactory struct {
	parser.ProviderFactory
	calls *atomic.Int32
}

func (f failureFingerprintFactory) NewProvider(cfg parser.ProviderConfig) parser.Provider {
	return failureFingerprintProvider{Provider: f.ProviderFactory.NewProvider(cfg), calls: f.calls}
}

type failureFingerprintProvider struct {
	parser.Provider
	calls *atomic.Int32
}

func (p failureFingerprintProvider) WatchRoots(ctx context.Context) ([]parser.WatchRoot, error) {
	return parser.ResolveWatchRoots(ctx, p.Provider)
}

func (p failureFingerprintProvider) Fingerprint(ctx context.Context, source parser.SourceRef) (parser.SourceFingerprint, error) {
	p.calls.Add(1)
	return p.Provider.Fingerprint(ctx, source)
}

func TestMissingGrokSummaryRetriesWhenCreated(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "project", "11111111-2222-4333-8444-555555555555", "summary.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	factory, ok := parser.ProviderFactoryByType(parser.AgentGrok)
	require.True(t, ok)
	require.NoError(t, os.WriteFile(path, []byte(`{}`), 0o600))
	provider := factory.NewProvider(parser.ProviderConfig{Roots: []string{root}})
	sources, err := provider.Discover(t.Context())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.NoError(t, os.Remove(path))
	var calls atomic.Int32
	database := openTestDB(t)
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentGrok: {root}}, Machine: "local",
		ProviderFactories: []parser.ProviderFactory{failureFingerprintFactory{factory, &calls}},
	})
	t.Cleanup(engine.Close)
	// A source can disappear between discovery and its fingerprint read.
	source := sources[0]
	plan := ChangedPathPlan{Files: []parser.DiscoveredFile{{
		Agent: parser.AgentGrok, Path: path, ProviderSource: &source, ProviderProcess: true,
	}}}
	for range 3 {
		result, err := engine.SyncChangedPathPlanContext(t.Context(), plan, nil)
		require.Error(t, err)
		assert.Equal(t, 1, result.Stats.Failed)
	}
	assert.Equal(t, int32(1), calls.Load())
	require.NoError(t, os.WriteFile(path, []byte(`{
		"info":{"id":"11111111-2222-4333-8444-555555555555","cwd":"/workspace/fixture"},
		"created_at":"2026-07-02T15:11:00Z","updated_at":"2026-07-02T15:12:00Z"
	}`), 0o600))
	result, err := engine.SyncChangedPathPlanContext(t.Context(), plan, nil)
	require.NoError(t, err)
	assert.Zero(t, result.Stats.Failed)
	assert.Equal(t, int32(2), calls.Load(), "the newly created source must retry immediately")
}

func TestSourceFailureCacheEvictsOldErrors(t *testing.T) {
	var cache sourceFailureCache
	for i := range sourceFailureCacheLimit + 1 {
		cache.put(sourceFailure{
			key: verifiedSourceKey{agent: parser.AgentCowork, path: fmt.Sprint(i)},
			err: errors.New("read failed"), expires: time.Now().Add(time.Hour),
		})
	}
	_, oldest := cache.get(verifiedSourceKey{agent: parser.AgentCowork, path: "0"})
	assert.False(t, oldest, "eviction must make an old failure eligible to retry")
	_, newest := cache.get(verifiedSourceKey{agent: parser.AgentCowork, path: fmt.Sprint(sourceFailureCacheLimit)})
	assert.True(t, newest)
}

type statFailureFactory struct {
	parser.ProviderFactory
	fingerprints atomic.Int32
	parses       atomic.Int32
	fail         atomic.Bool
}

func (f *statFailureFactory) NewProvider(cfg parser.ProviderConfig) parser.Provider {
	return statFailureProvider{Provider: f.ProviderFactory.NewProvider(cfg), factory: f}
}

type statFailureProvider struct {
	parser.Provider
	factory *statFailureFactory
}

func (p statFailureProvider) ComputeMultiFileStatHash(path string) uint64 {
	return p.Provider.(parser.MultiFileStatHasher).ComputeMultiFileStatHash(path)
}

func (p statFailureProvider) Fingerprint(ctx context.Context, source parser.SourceRef) (parser.SourceFingerprint, error) {
	p.factory.fingerprints.Add(1)
	return p.Provider.Fingerprint(ctx, source)
}

func (p statFailureProvider) Parse(ctx context.Context, req parser.ParseRequest) (parser.ParseOutcome, error) {
	p.factory.parses.Add(1)
	if p.factory.fail.Load() {
		return parser.ParseOutcome{}, io.ErrUnexpectedEOF
	}
	return p.Provider.Parse(ctx, req)
}

func TestUnchangedFailureDoesNotRehashTranscript(t *testing.T) {
	root := t.TempDir()
	writeGroupedClaudeFixture(t, root, "failed-read")
	path := filepath.Join(root, "project", "failed-read.jsonl")
	base, ok := parser.ProviderFactoryByType(parser.AgentClaude)
	require.True(t, ok)
	factory := &statFailureFactory{ProviderFactory: base}
	factory.fail.Store(true)
	database := openTestDB(t)
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, Machine: "local",
		ProviderFactories: []parser.ProviderFactory{factory},
	})
	t.Cleanup(engine.Close)
	if engine.providerStatHashers[parser.AgentClaude].ComputeMultiFileStatHash(path) == 0 {
		t.Skip("filesystem does not provide a reliable change time")
	}
	for range 3 {
		require.Error(t, engine.SyncPathsContext(t.Context(), []string{path}))
	}
	assert.Equal(t, int32(1), factory.fingerprints.Load(), "unchanged failed transcripts must not be read repeatedly")
	assert.Equal(t, int32(1), factory.parses.Load())

	factory.fail.Store(false)
	info, err := os.Stat(path)
	require.NoError(t, err)
	// An atomic replacement must invalidate the failed-source identity even
	// when the source's size and mtime are preserved.
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	replacement := path + ".replacement"
	require.NoError(t, os.WriteFile(replacement, content, 0o600))
	require.NoError(t, os.Chtimes(replacement, info.ModTime(), info.ModTime()))
	require.NoError(t, os.Rename(replacement, path))
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
	assert.Equal(t, int32(2), factory.parses.Load())
	session, err := database.GetSession(t.Context(), "failed-read")
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, 1, session.MessageCount)
}
