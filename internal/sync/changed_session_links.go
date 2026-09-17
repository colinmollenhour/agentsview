package sync

import (
	"errors"
	"fmt"
	"slices"
)

// changedSessionLinks collects only sessions reached by a changed-path batch.
type changedSessionLinks map[string]struct{}

func (ids changedSessionLinks) observe(job syncJob, prefix string) {
	if job.incremental != nil {
		ids[job.incremental.sessionID] = struct{}{}
	}
	for _, parsed := range job.results {
		ids[applyIDPrefixToID(prefix, parsed.Session.ID)] = struct{}{}
	}
	for _, id := range job.excludedSessionIDs {
		ids[applyIDPrefixToID(prefix, id)] = struct{}{}
	}
}

func (ids changedSessionLinks) link(e *Engine) error {
	if len(ids) == 0 {
		return nil
	}
	sessionIDs := make([]string, 0, len(ids))
	for id := range ids {
		sessionIDs = append(sessionIDs, id)
	}
	slices.Sort(sessionIDs)
	if err := e.db.LinkSubagentSessionsForSessions(sessionIDs); err != nil {
		linkErr := fmt.Errorf("link affected subagent sessions: %w", err)
		if queueErr := e.db.QueueSubagentParentRepairs(sessionIDs); queueErr != nil {
			return errors.Join(linkErr,
				fmt.Errorf("queue affected subagent parent repairs: %w", queueErr))
		}
		return linkErr
	}
	return nil
}
