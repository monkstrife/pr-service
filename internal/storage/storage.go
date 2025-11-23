package storage

import (
	"errors"
	"time"
)

var (
	ErrTeamExists  = errors.New("team already exists")
	ErrPRExists    = errors.New("pull request already exists")
	ErrNotFound    = errors.New("not found")
	ErrPRMerged    = errors.New("pull request already merged")
	ErrNotAssigned = errors.New("reviewer is not assigned to this PR")
	ErrNoCandidate = errors.New("no active replacement candidate in team")
)

type Repository interface {
	// Teams
	CreateTeam(teamName string, members []TeamMember) (Team, error)
	GetTeam(teamName string) (Team, error)

	// Users
	SetUserIsActive(userID string, isActive bool) (User, error)

	// PR
	CreatePullRequestWithAutoAssign(prID, prName, authorID string) (PullRequest, error)
	MergePullRequest(prID string) (PullRequest, error)
	ReassignReviewer(prID, oldUserID string) (PullRequest, string, error)
	GetUserReviews(userID string) (UserReviews, error)

	// Stats
	GetStats() (Stats, error)

	// Deactivate
	BulkDeactivateUsersAndReassign(teamName string, userIDs []string) (BulkDeactivateResult, error)
}

type TeamMember struct {
	UserID   string
	Username string
	IsActive bool
}

type Team struct {
	TeamName string
	Members  []TeamMember
}

type User struct {
	UserID   string
	Username string
	TeamName string
	IsActive bool
}

type PullRequest struct {
	ID                string
	Name              string
	AuthorID          string
	Status            string
	AssignedReviewers []string
	CreatedAt         *time.Time
	MergedAt          *time.Time
}

type PullRequestShort struct {
	ID       string
	Name     string
	AuthorID string
	Status   string
}

type UserReviews struct {
	UserID       string
	PullRequests []PullRequestShort
}

type ReviewerAssignmentStat struct {
	UserID        string
	AssignedCount int
}

type Stats struct {
	TotalPullRequests       int
	TotalOpenPullRequests   int
	TotalMergedPullRequests int
	AssignmentsByReviewer   []ReviewerAssignmentStat
}

type BulkDeactivateResult struct {
	TeamName           string
	DeactivatedUserIDs []string
	ReassignedCount    int // сколько раз удалось заменить ревьювера на другого
	RemovedAssignments int // сколько ревьюверов просто удалили, потому что кандидатов не было
}
