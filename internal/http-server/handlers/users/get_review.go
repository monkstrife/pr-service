package users

import (
	"errors"
	"net/http"

	"log/slog"

	resp "pr-service/internal/lib/api/response"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

// DTO

type GetReviewResponse struct {
	UserID       string                 `json:"user_id"`
	PullRequests []GetReviewPullRequest `json:"pull_requests"`
}

type GetReviewPullRequest struct {
	PullRequestID   string `json:"pull_request_id"`
	PullRequestName string `json:"pull_request_name"`
	AuthorID        string `json:"author_id"`
	Status          string `json:"status"`
}

// Handler

// GET /users/getReview?user_id=...
func GetReview(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.users.get_review"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			log.Warn("user_id query param is empty")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("user_id is required"))

			return
		}

		ur, err := repo.GetUserReviews(userID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				log.Info("user not found when getting reviews", slog.String("user_id", userID))

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return
			}

			log.Error("failed to get user reviews", sl.Err(err))

			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		res := GetReviewResponse{
			UserID:       ur.UserID,
			PullRequests: make([]GetReviewPullRequest, 0, len(ur.PullRequests)),
		}

		for _, pr := range ur.PullRequests {
			res.PullRequests = append(res.PullRequests, GetReviewPullRequest{
				PullRequestID:   pr.ID,
				PullRequestName: pr.Name,
				AuthorID:        pr.AuthorID,
				Status:          pr.Status,
			})
		}

		log.Info("user reviews fetched",
			slog.String("user_id", ur.UserID),
			slog.Int("pull_requests_count", len(res.PullRequests)),
		)

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}
