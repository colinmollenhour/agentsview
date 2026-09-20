package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

const grokRetryID = "11111111-2222-4333-8444-555555555555"
const grokRetrySummary = `{"info":{"id":"11111111-2222-4333-8444-555555555555","cwd":"/workspace/fixture"},"created_at":"2026-07-02T15:11:00Z","updated_at":"2026-07-02T15:12:00Z"}`

func TestGrokWatcherMissingSummaryDoesNotRepeatFingerprint(t *testing.T) {
	for _, companion := range []string{"signals.json", "chat_history.jsonl", "updates.jsonl", "prompt_context.json"} {
		for _, removed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/removed=%t", companion, removed), func(t *testing.T) {
				root := t.TempDir()
				dir := filepath.Join(root, "project", grokRetryID)
				require.NoError(t, os.MkdirAll(dir, 0o755))
				path := filepath.Join(dir, companion)
				body := "{}"
				if filepath.Ext(path) == ".jsonl" {
					body = ""
				}
				require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
				if removed {
					require.NoError(t, os.Remove(path))
				}
				factory, ok := parser.ProviderFactoryByType(parser.AgentGrok)
				require.True(t, ok)
				var calls atomic.Int32
				database := openTestDB(t)
				summary := filepath.Join(dir, "summary.json")
				require.NoError(t, database.UpsertSession(db.Session{
					ID: "grok:" + grokRetryID, Agent: "grok", Project: "fixture",
					Machine: "local", FilePath: &summary, MessageCount: 1,
				}))
				require.NoError(t, database.InsertMessages([]db.Message{{
					SessionID: "grok:" + grokRetryID, Ordinal: 0, Role: "user", Content: "archived prompt",
				}}))
				engine := NewEngine(database, EngineConfig{
					AgentDirs: map[parser.AgentType][]string{parser.AgentGrok: {root}}, Machine: "local",
					ProviderFactories: []parser.ProviderFactory{failureFingerprintFactory{factory, &calls}},
				})
				t.Cleanup(engine.Close)
				for range 3 {
					err := engine.SyncPathsContext(t.Context(), []string{path})
					if removed {
						require.Error(t, err)
						assert.Equal(t, 1, engine.LastSyncStats().Failed)
					} else {
						require.NoError(t, err, "writes without a discoverable summary remain unclassified")
					}
				}
				if removed {
					assert.Equal(t, int32(1), calls.Load(), "companion events must retain missing-summary suppression")
				} else {
					assert.Zero(t, calls.Load())
				}
				stored, err := database.GetSessionFull(t.Context(), "grok:"+grokRetryID)
				require.NoError(t, err)
				require.NotNil(t, stored)
				assert.Nil(t, stored.DeletedAt)
				assert.Nil(t, stored.SourceMissingAt)
				messages, err := database.GetAllMessages(t.Context(), stored.ID)
				require.NoError(t, err)
				require.Len(t, messages, 1)
				assert.Equal(t, "archived prompt", messages[0].Content)

				require.NoError(t, os.WriteFile(summary, []byte(grokRetrySummary), 0o600))
				calls.Store(0)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				assert.Equal(t, int32(1), calls.Load(), "creating the summary must permit immediate recovery")
				assert.Equal(t, 1, engine.LastSyncStats().Synced)
			})
		}
	}
}

func TestGrokWatcherCompanionChangesStillRefreshSession(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "project", grokRetryID)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	summary := filepath.Join(dir, "summary.json")
	history := filepath.Join(dir, "chat_history.jsonl")
	require.NoError(t, os.WriteFile(summary, []byte(grokRetrySummary), 0o600))
	writeHistory := func(content string) {
		t.Helper()
		require.NoError(t, os.WriteFile(history, []byte(fmt.Sprintf("{\"type\":\"user\",\"content\":%q}\n", content)), 0o600))
		stamp := time.Unix(1700000000, 0)
		require.NoError(t, os.Chtimes(history, stamp, stamp))
	}
	writeHistory("first")
	promptContext := filepath.Join(dir, "prompt_context.json")
	writeContext := func(body string) {
		t.Helper()
		require.NoError(t, os.WriteFile(promptContext, []byte(body), 0o600))
		stamp := time.Unix(1700000000, 0)
		require.NoError(t, os.Chtimes(promptContext, stamp, stamp))
	}
	writeContext(`{"is_non_interactive":false}`)
	database := openTestDB(t)
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentGrok: {root}}, Machine: "local",
	})
	t.Cleanup(engine.Close)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{history}))
	for range 3 {
		require.NoError(t, engine.SyncPathsContext(t.Context(), []string{history}))
		assert.Zero(t, engine.LastSyncStats().Synced, "unchanged companion events must not rewrite the session")
	}
	// Neither the summary nor the companion's size or mtime changes.
	writeContext(`{"is_non_interactive":true }`)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{promptContext}))
	assert.Equal(t, 1, engine.LastSyncStats().Synced)
	stored, err := database.GetSession(t.Context(), "grok:"+grokRetryID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.True(t, stored.IsAutomated)
	assert.Equal(t, "non-interactive", stored.SessionKind)
	file, err := os.OpenFile(history, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString("{\"type\":\"assistant\",\"content\":\"reply\"}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{history}))
	messages, err := database.GetAllMessages(t.Context(), "grok:"+grokRetryID)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	assert.Equal(t, "first", messages[0].Content)
	assert.Equal(t, "reply", messages[1].Content)
	require.NoError(t, os.Remove(history))
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{history}))
	messages, err = database.GetAllMessages(t.Context(), "grok:"+grokRetryID)
	require.NoError(t, err)
	require.Len(t, messages, 2, "losing a companion must preserve archived messages")
	stored, err = database.GetSessionFull(t.Context(), "grok:"+grokRetryID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Nil(t, stored.SourceMissingAt, "a companion deletion must not tombstone the summary")
}

func TestGrokWatcherFailureWorkIsIndependentOfArchiveSize(t *testing.T) {
	for _, count := range []int{8, 800} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			root := t.TempDir()
			database := openTestDB(t)
			for i := range count {
				path := filepath.Join(root, "archive", fmt.Sprintf("%d.jsonl", i))
				id := fmt.Sprintf("archived-%d", i)
				require.NoError(t, database.UpsertSession(db.Session{
					ID: id, Agent: "claude", Project: "fixture", Machine: "local", FilePath: &path,
				}))
				if i%2 == 0 {
					require.NoError(t, database.SoftDeleteSession(id))
				}
			}
			grokRoot := filepath.Join(root, "grok")
			paths := make([]string, 33)
			for i := range paths {
				paths[i] = filepath.Join(grokRoot, "project", fmt.Sprintf("session-%d", i), "signals.json")
				require.NoError(t, os.MkdirAll(filepath.Dir(paths[i]), 0o755))
				require.NoError(t, os.WriteFile(paths[i], []byte("{}"), 0o600))
				require.NoError(t, os.Remove(paths[i]))
			}
			factory, ok := parser.ProviderFactoryByType(parser.AgentGrok)
			require.True(t, ok)
			var calls atomic.Int32
			engine := NewEngine(database, EngineConfig{
				AgentDirs: map[parser.AgentType][]string{parser.AgentGrok: {grokRoot}}, Machine: "local",
				ProviderFactories: []parser.ProviderFactory{failureFingerprintFactory{factory, &calls}},
			})
			t.Cleanup(engine.Close)
			for range 3 {
				require.Error(t, engine.SyncPathsContext(t.Context(), paths))
				assert.Equal(t, 33, engine.LastSyncStats().Failed)
			}
			assert.Equal(t, int32(33), calls.Load(), "each missing summary is attempted once across repeated batches")
			plan, err := engine.PlanChangedPathsContext(t.Context(), paths[:1])
			require.NoError(t, err)
			require.Len(t, plan.Files, 1)
			plan.Files[0].ForceParse = true
			_, err = engine.SyncChangedPathPlanContext(t.Context(), plan, nil)
			require.Error(t, err)
			assert.Equal(t, int32(34), calls.Load(), "an explicit forced parse must still retry")
			for _, id := range []string{"archived-0", "archived-1"} {
				stored, err := database.GetSessionFull(t.Context(), id)
				require.NoError(t, err)
				require.NotNil(t, stored)
				assert.Nil(t, stored.SourceMissingAt)
				if id == "archived-0" {
					assert.NotNil(t, stored.DeletedAt)
				} else {
					assert.Nil(t, stored.DeletedAt)
				}
			}
		})
	}
}
