package sqlite

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"pr-service/internal/storage"
	"time"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.sqlite.New"

	db, err := sql.Open("sqlite3", storagePath)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	// Включаем поддержку foreign key в SQLite
	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		return nil, fmt.Errorf("%s: enable foreign_keys: %w", op, err)
	}

	schema := `
-- teams
CREATE TABLE IF NOT EXISTS teams (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE
);

-- users
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT NOT NULL UNIQUE,   -- внешний id из API (u1, u2, ...)
    username    TEXT NOT NULL,
    team_id     INTEGER NOT NULL,
    is_active   INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

-- pull_requests
CREATE TABLE IF NOT EXISTS pull_requests (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pull_request_id  TEXT NOT NULL UNIQUE, -- внешний id из API (pr-1001)
    name             TEXT NOT NULL,
    author_id        INTEGER NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('OPEN', 'MERGED')),
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    merged_at        DATETIME NULL,
    FOREIGN KEY (author_id) REFERENCES users(id)
);

-- pr_reviewers (многие ко многим: PR <-> ревьюверы)
CREATE TABLE IF NOT EXISTS pr_reviewers (
    pr_id       INTEGER NOT NULL,
    reviewer_id INTEGER NOT NULL,
    PRIMARY KEY (pr_id, reviewer_id),
    FOREIGN KEY (pr_id) REFERENCES pull_requests(id) ON DELETE CASCADE,
    FOREIGN KEY (reviewer_id) REFERENCES users(id)
);

-- индексы для производительности
CREATE INDEX IF NOT EXISTS idx_users_team_id
    ON users(team_id);

CREATE INDEX IF NOT EXISTS idx_users_user_id
    ON users(user_id);

CREATE INDEX IF NOT EXISTS idx_pr_pull_request_id
    ON pull_requests(pull_request_id);

CREATE INDEX IF NOT EXISTS idx_pr_author_id
    ON pull_requests(author_id);

CREATE INDEX IF NOT EXISTS idx_pr_reviewers_reviewer_id
    ON pr_reviewers(reviewer_id);
`

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

// получаем PL
func (s *Storage) getPullRequestByExternalID(prID string) (storage.PullRequest, error) {
	const op = "storage.sqlite.getPullRequestByExternalID"

	// читаем сам PR + автора
	row := s.db.QueryRow(`
        SELECT pr.pull_request_id,
               pr.name,
               au.user_id,      -- внешний author_id
               pr.status,
               pr.created_at,
               pr.merged_at
        FROM pull_requests pr
        JOIN users au ON pr.author_id = au.id
        WHERE pr.pull_request_id = ?`,
		prID,
	)

	var (
		prExternalID     string
		name             string
		authorExternalID string
		status           string
		createdAt        sql.NullTime
		mergedAt         sql.NullTime
	)

	if err := row.Scan(&prExternalID, &name, &authorExternalID, &status, &createdAt, &mergedAt); err != nil {
		if err == sql.ErrNoRows {
			return storage.PullRequest{}, storage.ErrNotFound
		}
		return storage.PullRequest{}, fmt.Errorf("%s: scan pr: %w", op, err)
	}

	// читаем назначенных ревьюверов (user_id)
	rRows, err := s.db.Query(`
        SELECT u.user_id
        FROM pr_reviewers r
        JOIN users u ON r.reviewer_id = u.id
        JOIN pull_requests pr ON r.pr_id = pr.id
        WHERE pr.pull_request_id = ?`,
		prID,
	)
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: query reviewers: %w", op, err)
	}
	defer rRows.Close()

	reviewers := make([]string, 0, 2)
	for rRows.Next() {
		var uid string
		if err := rRows.Scan(&uid); err != nil {
			return storage.PullRequest{}, fmt.Errorf("%s: scan reviewer: %w", op, err)
		}
		reviewers = append(reviewers, uid)
	}
	if err := rRows.Err(); err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: reviewers rows err: %w", op, err)
	}

	// конвертим sql.NullTime в *time.Time
	var createdPtr *time.Time
	if createdAt.Valid {
		t := createdAt.Time
		createdPtr = &t
	}

	var mergedPtr *time.Time
	if mergedAt.Valid {
		t := mergedAt.Time
		mergedPtr = &t
	}

	return storage.PullRequest{
		ID:                prExternalID,
		Name:              name,
		AuthorID:          authorExternalID,
		Status:            status,
		AssignedReviewers: reviewers,
		CreatedAt:         createdPtr,
		MergedAt:          mergedPtr,
	}, nil
}

// users
func (s *Storage) SetUserIsActive(userID string, isActive bool) (storage.User, error) {
	const op = "storage.sqlite.SetUserIsActive"

	res, err := s.db.Exec("UPDATE users SET is_active = ? WHERE user_id = ?", boolToInt(isActive), userID)
	if err != nil {
		return storage.User{}, fmt.Errorf("%s: %w", op, err)
	}

	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storage.User{}, storage.ErrNotFound
	}

	// прочитать юзера вместе с командой
	row := s.db.QueryRow(`
        SELECT u.user_id, u.username, t.name, u.is_active
        FROM users u
        JOIN teams t ON u.team_id = t.id
        WHERE u.user_id = ?`, userID)

	var uid, username, teamName string
	var activeInt int
	if err := row.Scan(&uid, &username, &teamName, &activeInt); err != nil {
		return storage.User{}, fmt.Errorf("%s: %w", op, err)
	}

	return storage.User{
		UserID:   uid,
		Username: username,
		TeamName: teamName,
		IsActive: activeInt == 1,
	}, nil
}

// pr
func (s *Storage) CreatePullRequestWithAutoAssign(prID, prName, authorExternalID string) (storage.PullRequest, error) {
	const op = "storage.sqlite.CreatePullRequestWithAutoAssign"

	tx, err := s.db.Begin()
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback()

	// нет ли уже такого PR
	var tmp int
	err = tx.QueryRow("SELECT 1 FROM pull_requests WHERE pull_request_id = ?", prID).Scan(&tmp)
	if err == nil {
		return storage.PullRequest{}, storage.ErrPRExists
	}
	if err != sql.ErrNoRows {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}

	// найти автора
	var authorID, teamID int64
	err = tx.QueryRow(`
        SELECT u.id, u.team_id FROM users u WHERE u.user_id = ?`, authorExternalID,
	).Scan(&authorID, &teamID)
	if err == sql.ErrNoRows {
		return storage.PullRequest{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}

	// кандидаты: активные из команды автора, не он сам
	rows, err := tx.Query(`
        SELECT user_id, id
        FROM users
        WHERE team_id = ? AND is_active = 1 AND id != ?`, teamID, authorID)
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	type cand struct {
		extID string
		id    int64
	}
	var candidates []cand
	for rows.Next() {
		var uid string
		var id int64
		if err := rows.Scan(&uid, &id); err != nil {
			return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
		}
		candidates = append(candidates, cand{extID: uid, id: id})
	}

	// перемешать и взять до 2
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	if len(candidates) > 2 {
		candidates = candidates[:2]
	}

	// создать PR
	res, err := tx.Exec(`
        INSERT INTO pull_requests(pull_request_id, name, author_id, status)
        VALUES(?, ?, ?, 'OPEN')`, prID, prName, authorID)
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}
	prIntID, _ := res.LastInsertId()

	// вставить ревьюверов
	for _, c := range candidates {
		if _, err := tx.Exec(`INSERT INTO pr_reviewers(pr_id, reviewer_id) VALUES(?, ?)`, prIntID, c.id); err != nil {
			return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}

	// собрать полный объект
	return s.getPullRequestByExternalID(prID)
}

func (s *Storage) MergePullRequest(prID string) (storage.PullRequest, error) {
	const op = "storage.sqlite.MergePullRequest"

	tx, err := s.db.Begin()
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback()

	var intID int64
	var status string
	err = tx.QueryRow(`
        SELECT id, status FROM pull_requests WHERE pull_request_id = ?`, prID,
	).Scan(&intID, &status)
	if err == sql.ErrNoRows {
		return storage.PullRequest{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}

	if status != "MERGED" {
		_, err = tx.Exec(`
            UPDATE pull_requests
            SET status = 'MERGED', merged_at = CURRENT_TIMESTAMP
            WHERE id = ?`, intID)
		if err != nil {
			return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return storage.PullRequest{}, fmt.Errorf("%s: %w", op, err)
	}

	return s.getPullRequestByExternalID(prID)
}

func (s *Storage) ReassignReviewer(prID, oldUserID string) (storage.PullRequest, string, error) {
	const op = "storage.sqlite.ReassignReviewer"

	tx, err := s.db.Begin()
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback()

	// найти PR
	var prIntID, authorIntID int64
	var status string
	err = tx.QueryRow(`
        SELECT id, author_id, status FROM pull_requests WHERE pull_request_id = ?`, prID,
	).Scan(&prIntID, &authorIntID, &status)
	if err == sql.ErrNoRows {
		return storage.PullRequest{}, "", storage.ErrNotFound
	}
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}

	if status == "MERGED" {
		return storage.PullRequest{}, "", storage.ErrPRMerged
	}

	// найти старого ревьювера
	var oldIntID, teamID int64
	err = tx.QueryRow(`
        SELECT u.id, u.team_id
        FROM users u
        WHERE u.user_id = ?`, oldUserID,
	).Scan(&oldIntID, &teamID)
	if err == sql.ErrNoRows {
		return storage.PullRequest{}, "", storage.ErrNotFound
	}
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}

	// проверить, что он назначен
	var tmp int
	err = tx.QueryRow(`
        SELECT 1 FROM pr_reviewers
        WHERE pr_id = ? AND reviewer_id = ?`, prIntID, oldIntID,
	).Scan(&tmp)
	if err == sql.ErrNoRows {
		return storage.PullRequest{}, "", storage.ErrNotAssigned
	}
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}

	// собрать уже назначенных ревьюверов
	assigned := map[int64]struct{}{}
	rows, err := tx.Query(`SELECT reviewer_id FROM pr_reviewers WHERE pr_id = ?`, prIntID)
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()
	for rows.Next() {
		var rID int64
		if err := rows.Scan(&rID); err != nil {
			return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
		}
		assigned[rID] = struct{}{}
	}

	// кандидаты: команда старого ревьювера
	candRows, err := tx.Query(`
        SELECT id, user_id
        FROM users
        WHERE team_id = ? AND is_active = 1`, teamID)
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}
	defer candRows.Close()

	type cand struct {
		id    int64
		extID string
	}
	var candidates []cand
	for candRows.Next() {
		var id int64
		var extID string
		if err := candRows.Scan(&id, &extID); err != nil {
			return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
		}
		if id == oldIntID {
			continue
		}
		if id == authorIntID {
			continue
		}
		if _, ok := assigned[id]; ok {
			continue
		}
		candidates = append(candidates, cand{id: id, extID: extID})
	}

	if len(candidates) == 0 {
		return storage.PullRequest{}, "", storage.ErrNoCandidate
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	chosen := candidates[0]

	// заменить
	_, err = tx.Exec(`
        UPDATE pr_reviewers
        SET reviewer_id = ?
        WHERE pr_id = ? AND reviewer_id = ?`, chosen.id, prIntID, oldIntID)
	if err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}

	if err := tx.Commit(); err != nil {
		return storage.PullRequest{}, "", fmt.Errorf("%s: %w", op, err)
	}

	pr, err := s.getPullRequestByExternalID(prID)
	if err != nil {
		return storage.PullRequest{}, "", err
	}
	return pr, chosen.extID, nil
}

func (s *Storage) GetUserReviews(userID string) (storage.UserReviews, error) {
	const op = "storage.sqlite.GetUserReviews"

	// проверим наличие юзера
	var tmp int
	err := s.db.QueryRow(`SELECT 1 FROM users WHERE user_id = ?`, userID).Scan(&tmp)
	if err == sql.ErrNoRows {
		return storage.UserReviews{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.UserReviews{}, fmt.Errorf("%s: %w", op, err)
	}

	rows, err := s.db.Query(`
        SELECT pr.pull_request_id, pr.name, au.user_id, pr.status
        FROM pr_reviewers r
        JOIN pull_requests pr ON r.pr_id = pr.id
        JOIN users au ON pr.author_id = au.id
        JOIN users u ON r.reviewer_id = u.id
        WHERE u.user_id = ?`, userID)
	if err != nil {
		return storage.UserReviews{}, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	prs := []storage.PullRequestShort{}
	for rows.Next() {
		var prID, name, authorExtID, status string
		if err := rows.Scan(&prID, &name, &authorExtID, &status); err != nil {
			return storage.UserReviews{}, fmt.Errorf("%s: %w", op, err)
		}
		prs = append(prs, storage.PullRequestShort{
			ID:       prID,
			Name:     name,
			AuthorID: authorExtID,
			Status:   status,
		})
	}

	return storage.UserReviews{
		UserID:       userID,
		PullRequests: prs,
	}, nil
}

// teams
func (s *Storage) CreateTeam(teamName string, members []storage.TeamMember) (storage.Team, error) {
	const op = "storage.sqlite.CreateTeam"

	tx, err := s.db.Begin()
	if err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback()

	// Проверяем, что команды с таким именем ещё нет
	var existingID int64
	err = tx.QueryRow(`SELECT id FROM teams WHERE name = ?`, teamName).Scan(&existingID)
	if err == nil {
		// уже есть команда
		return storage.Team{}, storage.ErrTeamExists
	}
	if err != nil && err != sql.ErrNoRows {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}

	// Создаём команду
	res, err := tx.Exec(`INSERT INTO teams(name) VALUES(?)`, teamName)
	if err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}
	teamID, _ := res.LastInsertId()

	// Создаём или обновляем пользователей команды
	for _, m := range members {
		var userIntID int64
		err = tx.QueryRow(`SELECT id FROM users WHERE user_id = ?`, m.UserID).Scan(&userIntID)
		if err == sql.ErrNoRows {
			// создаём нового пользователя
			_, err = tx.Exec(
				`INSERT INTO users(user_id, username, team_id, is_active)
                 VALUES(?, ?, ?, ?)`,
				m.UserID, m.Username, teamID, boolToInt(m.IsActive),
			)
			if err != nil {
				return storage.Team{}, fmt.Errorf("%s: %w", op, err)
			}
		} else if err == nil {
			// обновляем существующего пользователя
			_, err = tx.Exec(
				`UPDATE users
                 SET username = ?, team_id = ?, is_active = ?
                 WHERE user_id = ?`,
				m.Username, teamID, boolToInt(m.IsActive), m.UserID,
			)
			if err != nil {
				return storage.Team{}, fmt.Errorf("%s: %w", op, err)
			}
		} else {
			return storage.Team{}, fmt.Errorf("%s: %w", op, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}

	// Возвращаем актуальное состояние команды из БД
	return s.GetTeam(teamName)
}

func (s *Storage) GetTeam(teamName string) (storage.Team, error) {
	const op = "storage.sqlite.GetTeam"

	// Находим команду
	var teamID int64
	err := s.db.QueryRow(`SELECT id FROM teams WHERE name = ?`, teamName).Scan(&teamID)
	if err == sql.ErrNoRows {
		return storage.Team{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}

	// Выбираем всех пользователей команды
	rows, err := s.db.Query(
		`SELECT user_id, username, is_active
         FROM users
         WHERE team_id = ?`,
		teamID,
	)
	if err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	members := make([]storage.TeamMember, 0)
	for rows.Next() {
		var (
			userID    string
			username  string
			isActiveI int
		)
		if err := rows.Scan(&userID, &username, &isActiveI); err != nil {
			return storage.Team{}, fmt.Errorf("%s: %w", op, err)
		}
		members = append(members, storage.TeamMember{
			UserID:   userID,
			Username: username,
			IsActive: isActiveI == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return storage.Team{}, fmt.Errorf("%s: %w", op, err)
	}

	return storage.Team{
		TeamName: teamName,
		Members:  members,
	}, nil
}

// Stats
func (s *Storage) GetStats() (storage.Stats, error) {
	const op = "storage.sqlite.GetStats"

	var stats storage.Stats

	// Общее количество PR
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pull_requests`).Scan(&stats.TotalPullRequests); err != nil {
		return storage.Stats{}, fmt.Errorf("%s: count total prs: %w", op, err)
	}

	// Кол-во OPEN
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pull_requests WHERE status = 'OPEN'`).Scan(&stats.TotalOpenPullRequests); err != nil {
		return storage.Stats{}, fmt.Errorf("%s: count open prs: %w", op, err)
	}

	// Кол-во MERGED
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pull_requests WHERE status = 'MERGED'`).Scan(&stats.TotalMergedPullRequests); err != nil {
		return storage.Stats{}, fmt.Errorf("%s: count merged prs: %w", op, err)
	}

	// Кол-во назначений по ревьюверам
	rows, err := s.db.Query(`
        SELECT u.user_id, COUNT(*) AS assigned_count
        FROM pr_reviewers r
        JOIN users u ON r.reviewer_id = u.id
        GROUP BY u.user_id
        ORDER BY assigned_count DESC, u.user_id ASC
    `)
	if err != nil {
		return storage.Stats{}, fmt.Errorf("%s: query reviewer stats: %w", op, err)
	}
	defer rows.Close()

	var reviewerStats []storage.ReviewerAssignmentStat
	for rows.Next() {
		var userID string
		var count int
		if err := rows.Scan(&userID, &count); err != nil {
			return storage.Stats{}, fmt.Errorf("%s: scan reviewer stats: %w", op, err)
		}
		reviewerStats = append(reviewerStats, storage.ReviewerAssignmentStat{
			UserID:        userID,
			AssignedCount: count,
		})
	}
	if err := rows.Err(); err != nil {
		return storage.Stats{}, fmt.Errorf("%s: reviewer stats rows err: %w", op, err)
	}

	stats.AssignmentsByReviewer = reviewerStats

	return stats, nil
}

// Deactivate
func (s *Storage) BulkDeactivateUsersAndReassign(teamName string, userIDs []string) (storage.BulkDeactivateResult, error) {
	const op = "storage.sqlite.BulkDeactivateUsersAndReassign"

	if len(userIDs) == 0 {
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: empty userIDs", op)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: begin tx: %w", op, err)
	}
	defer tx.Rollback()

	// Найти команду
	var teamID int64
	if err := tx.QueryRow(`SELECT id FROM teams WHERE name = ?`, teamName).Scan(&teamID); err != nil {
		if err == sql.ErrNoRows {
			return storage.BulkDeactivateResult{}, storage.ErrNotFound
		}
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: select team: %w", op, err)
	}

	// Найти пользователей этой команды по внешним user_id
	type userInfo struct {
		id    int64
		extID string
	}
	deactivated := make([]userInfo, 0, len(userIDs))

	for _, uid := range userIDs {
		var intID int64
		if err := tx.QueryRow(
			`SELECT id FROM users WHERE team_id = ? AND user_id = ?`,
			teamID, uid,
		).Scan(&intID); err != nil {
			if err == sql.ErrNoRows {
				return storage.BulkDeactivateResult{}, storage.ErrNotFound
			}
			return storage.BulkDeactivateResult{}, fmt.Errorf("%s: select user %s: %w", op, uid, err)
		}
		deactivated = append(deactivated, userInfo{id: intID, extID: uid})
	}

	// Деактивировать этих пользователей
	for _, u := range deactivated {
		if _, err := tx.Exec(`UPDATE users SET is_active = 0 WHERE id = ?`, u.id); err != nil {
			return storage.BulkDeactivateResult{}, fmt.Errorf("%s: deactivate user %d: %w", op, u.id, err)
		}
	}

	// Предзагрузить активных пользователей команды для последующих замен
	activeRows, err := tx.Query(`
        SELECT id
        FROM users
        WHERE team_id = ? AND is_active = 1
    `, teamID)
	if err != nil {
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: query active users: %w", op, err)
	}
	defer activeRows.Close()

	activeUserIDs := make([]int64, 0)
	for activeRows.Next() {
		var id int64
		if err := activeRows.Scan(&id); err != nil {
			return storage.BulkDeactivateResult{}, fmt.Errorf("%s: scan active user: %w", op, err)
		}
		activeUserIDs = append(activeUserIDs, id)
	}
	if err := activeRows.Err(); err != nil {
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: active users rows err: %w", op, err)
	}

	reassignedCount := 0
	removedCount := 0

	for _, u := range deactivated {
		prRows, err := tx.Query(`
            SELECT pr.id, pr.author_id
            FROM pr_reviewers r
            JOIN pull_requests pr ON r.pr_id = pr.id
            WHERE r.reviewer_id = ? AND pr.status = 'OPEN'
        `, u.id)
		if err != nil {
			return storage.BulkDeactivateResult{}, fmt.Errorf("%s: query prs for reviewer %d: %w", op, u.id, err)
		}

		for prRows.Next() {
			var prIntID, authorIntID int64
			if err := prRows.Scan(&prIntID, &authorIntID); err != nil {
				prRows.Close()
				return storage.BulkDeactivateResult{}, fmt.Errorf("%s: scan pr for reviewer %d: %w", op, u.id, err)
			}

			assignedRows, err := tx.Query(`
                SELECT reviewer_id FROM pr_reviewers WHERE pr_id = ?
            `, prIntID)
			if err != nil {
				prRows.Close()
				return storage.BulkDeactivateResult{}, fmt.Errorf("%s: query assigned reviewers: %w", op, err)
			}

			assigned := make(map[int64]struct{})
			for assignedRows.Next() {
				var rid int64
				if err := assignedRows.Scan(&rid); err != nil {
					assignedRows.Close()
					prRows.Close()
					return storage.BulkDeactivateResult{}, fmt.Errorf("%s: scan assigned reviewer: %w", op, err)
				}
				assigned[rid] = struct{}{}
			}
			assignedRows.Close()
			if err := assignedRows.Err(); err != nil {
				prRows.Close()
				return storage.BulkDeactivateResult{}, fmt.Errorf("%s: assigned rows err: %w", op, err)
			}

			delete(assigned, u.id)

			candidates := make([]int64, 0, len(activeUserIDs))
			for _, candID := range activeUserIDs {
				if candID == authorIntID {
					continue
				}
				if _, ok := assigned[candID]; ok {
					continue
				}
				candidates = append(candidates, candID)
			}

			if len(candidates) == 0 {
				if _, err := tx.Exec(
					`DELETE FROM pr_reviewers WHERE pr_id = ? AND reviewer_id = ?`,
					prIntID, u.id,
				); err != nil {
					prRows.Close()
					return storage.BulkDeactivateResult{}, fmt.Errorf("%s: delete reviewer from pr: %w", op, err)
				}
				removedCount++
			} else {
				rand.Shuffle(len(candidates), func(i, j int) {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				})
				chosen := candidates[0]

				if _, err := tx.Exec(
					`UPDATE pr_reviewers SET reviewer_id = ? WHERE pr_id = ? AND reviewer_id = ?`,
					chosen, prIntID, u.id,
				); err != nil {
					prRows.Close()
					return storage.BulkDeactivateResult{}, fmt.Errorf("%s: update reviewer in pr: %w", op, err)
				}
				reassignedCount++
			}
		}
		prRows.Close()
		if err := prRows.Err(); err != nil {
			return storage.BulkDeactivateResult{}, fmt.Errorf("%s: prs rows err: %w", op, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return storage.BulkDeactivateResult{}, fmt.Errorf("%s: commit: %w", op, err)
	}

	res := storage.BulkDeactivateResult{
		TeamName:           teamName,
		DeactivatedUserIDs: make([]string, 0, len(deactivated)),
		ReassignedCount:    reassignedCount,
		RemovedAssignments: removedCount,
	}
	for _, u := range deactivated {
		res.DeactivatedUserIDs = append(res.DeactivatedUserIDs, u.extID)
	}

	return res, nil
}


func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
