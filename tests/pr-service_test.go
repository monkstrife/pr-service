package tests

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gavv/httpexpect/v2"
)

const host = "localhost:8080"

// базовый E2E-сценарий:
// - создаём команду
// - создаём PR с автоназначением ревьюверов
// - если есть ревьюверы, проверяем, что они видят PR в /users/getReview
// - делаем merge
// - проверяем, что reassign после MERGED даёт PR_MERGED
func TestPRService_E2E_BasicFlow(t *testing.T) {
	u := url.URL{
		Scheme: "http",
		Host:   host,
	}
	e := httpexpect.Default(t, u.String())

	suffix := time.Now().UnixNano()
	teamName := fmt.Sprintf("team-basic-%d", suffix)
	prID := fmt.Sprintf("pr-basic-%d", suffix)

	teamReq := map[string]any{
		"team_name": teamName,
		"members": []map[string]any{
			{"user_id": "u1", "username": "Alice", "is_active": true},
			{"user_id": "u2", "username": "Bob", "is_active": true},
			{"user_id": "u3", "username": "Charlie", "is_active": true},
		},
	}

	teamResp := e.POST("/team/add").
		WithJSON(teamReq).
		Expect().
		Status(http.StatusCreated).
		JSON().
		Object()

	teamObj := teamResp.Value("team").Object()
	teamObj.Value("team_name").String().IsEqual(teamName)
	teamObj.Value("members").Array().Length().IsEqual(3)

	createReq := map[string]any{
		"pull_request_id":   prID,
		"pull_request_name": "Integration test PR",
		"author_id":         "u1",
	}

	prResp := e.POST("/pullRequest/create").
		WithJSON(createReq).
		Expect().
		Status(http.StatusCreated).
		JSON().
		Object()

	prObj := prResp.Value("pr").Object()
	prObj.Value("pull_request_id").String().IsEqual(prID)
	prObj.Value("status").String().IsEqual("OPEN")

	reviewersArr := prObj.Value("assigned_reviewers").Array()
	reviewersArr.Length().Le(2)

	var firstReviewerID string
	if int(reviewersArr.Length().Raw()) > 0 {
		firstReviewerID = reviewersArr.Element(0).String().Raw()
	}

	if firstReviewerID != "" {
		reviewsResp := e.GET("/users/getReview").
			WithQuery("user_id", firstReviewerID).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object()

		reviewsResp.Value("user_id").String().IsEqual(firstReviewerID)

		prsArr := reviewsResp.Value("pull_requests").Array()
		found := false
		for i := 0; i < int(prsArr.Length().Raw()); i++ {
			item := prsArr.Element(i).Object()
			id := item.Value("pull_request_id").String().Raw()
			if id == prID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected pull request %s in /users/getReview for %s", prID, firstReviewerID)
		}
	} else {
		t.Log("no reviewers assigned to PR; skipping /users/getReview check")
	}

	mergeReq := map[string]any{
		"pull_request_id": prID,
	}

	mergeResp := e.POST("/pullRequest/merge").
		WithJSON(mergeReq).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object()

	mergePR := mergeResp.Value("pr").Object()
	mergePR.Value("pull_request_id").String().IsEqual(prID)
	mergePR.Value("status").String().IsEqual("MERGED")

	if firstReviewerID != "" {
		reassignReq := map[string]any{
			"pull_request_id": prID,
			"old_user_id":     firstReviewerID,
		}

		errResp := e.POST("/pullRequest/reassign").
			WithJSON(reassignReq).
			Expect().
			Status(http.StatusConflict).
			JSON().
			Object()

		errObj := errResp.Value("error").Object()
		errObj.Value("code").String().IsEqual("PR_MERGED")
	} else {
		t.Log("no reviewers assigned to PR; skipping /pullRequest/reassign after MERGED check")
	}
}

// второй сценарий:
// - создаём команду
// - открываем несколько PR от автора
// - деактивируем часть пользователей команды через /team/deactivateUsers
// - проверяем, что у деактивированных ревьюверов больше нет назначенных PR
func TestPRService_E2E_BulkDeactivate(t *testing.T) {
	u := url.URL{
		Scheme: "http",
		Host:   host,
	}
	e := httpexpect.Default(t, u.String())

	suffix := time.Now().UnixNano()
	teamName := fmt.Sprintf("team-bulk-%d", suffix)

	teamReq := map[string]any{
		"team_name": teamName,
		"members": []map[string]any{
			{"user_id": "bu1", "username": "BulkUser1", "is_active": true},
			{"user_id": "bu2", "username": "BulkUser2", "is_active": true},
			{"user_id": "bu3", "username": "BulkUser3", "is_active": true},
		},
	}

	e.POST("/team/add").
		WithJSON(teamReq).
		Expect().
		Status(http.StatusCreated)

	for i := range 5 {
		prID := fmt.Sprintf("pr-bulk-%d-%d", suffix, i)
		createReq := map[string]any{
			"pull_request_id":   prID,
			"pull_request_name": fmt.Sprintf("Bulk test PR %d", i),
			"author_id":         "bu1",
		}

		e.POST("/pullRequest/create").
			WithJSON(createReq).
			Expect().
			Status(http.StatusCreated)
	}

	deactivateReq := map[string]any{
		"team_name": teamName,
		"user_ids":  []string{"bu2", "bu3"},
	}

	deactResp := e.POST("/team/deactivateUsers").
		WithJSON(deactivateReq).
		Expect().
		Status(http.StatusOK).
		JSON().
		Object()

	deactResp.Value("team_name").String().IsEqual(teamName)
	deactResp.Value("deactivated_user_ids").Array().ContainsOnly("bu2", "bu3")

	for _, uid := range []string{"bu2", "bu3"} {
		reviewsResp := e.GET("/users/getReview").
			WithQuery("user_id", uid).
			Expect().
			Status(http.StatusOK).
			JSON().
			Object()

		reviewsResp.Value("user_id").String().IsEqual(uid)
		reviewsResp.Value("pull_requests").Array().Length().IsEqual(0)
	}
}
