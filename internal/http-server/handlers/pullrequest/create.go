package pullrequest

import (
	"errors"
	"net/http"
	"time"

	"log/slog"

	resp "pr-service/internal/lib/api/response"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

// DTO

type CreateRequest struct {
	PullRequestID   string `json:"pull_request_id"`
	PullRequestName string `json:"pull_request_name"`
	AuthorID        string `json:"author_id"`
}

type CreateResponse struct {
	PR PRResponse `json:"pr"`
}

type PRResponse struct {
	PullRequestID     string     `json:"pull_request_id"`
	PullRequestName   string     `json:"pull_request_name"`
	AuthorID          string     `json:"author_id"`
	Status            string     `json:"status"`
	AssignedReviewers []string   `json:"assigned_reviewers"`
	CreatedAt         *time.Time `json:"createdAt,omitempty"`
	MergedAt          *time.Time `json:"mergedAt,omitempty"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Handler

// POST /pullRequest/create
func Create(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.pullrequest.create"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req CreateRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode request body", sl.Err(err))

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("invalid request body"))

			return
		}

		if req.PullRequestID == "" || req.PullRequestName == "" || req.AuthorID == "" {
			log.Warn("missing required fields",
				slog.String("pull_request_id", req.PullRequestID),
				slog.String("pull_request_name", req.PullRequestName),
				slog.String("author_id", req.AuthorID),
			)

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("pull_request_id, pull_request_name and author_id are required"))

			return
		}

		pr, err := repo.CreatePullRequestWithAutoAssign(req.PullRequestID, req.PullRequestName, req.AuthorID)
		if err != nil {
			switch {
			case errors.Is(err, storage.ErrPRExists):
				log.Info("pull request already exists", slog.String("pull_request_id", req.PullRequestID))

				render.Status(r, http.StatusConflict)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "PR_EXISTS",
						Message: "PR id already exists",
					},
				})

				return

			case errors.Is(err, storage.ErrNotFound):
				// Автор или его команда не найдены
				log.Info("author or team not found when creating PR",
					slog.String("pull_request_id", req.PullRequestID),
					slog.String("author_id", req.AuthorID),
				)

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return

			default:
				log.Error("failed to create pull request", sl.Err(err))

				render.Status(r, http.StatusInternalServerError)
				render.JSON(w, r, resp.Error("internal error"))

				return
			}
		}

		res := CreateResponse{
			PR: mapPullRequestToResponse(pr),
		}

		log.Info("pull request created",
			slog.String("pull_request_id", pr.ID),
			slog.String("author_id", pr.AuthorID),
			slog.String("status", pr.Status),
		)

		render.Status(r, http.StatusCreated)
		render.JSON(w, r, res)
	}
}

func mapPullRequestToResponse(pr storage.PullRequest) PRResponse {
	return PRResponse{
		PullRequestID:     pr.ID,
		PullRequestName:   pr.Name,
		AuthorID:          pr.AuthorID,
		Status:            pr.Status,
		AssignedReviewers: pr.AssignedReviewers,
		CreatedAt:         pr.CreatedAt,
		MergedAt:          pr.MergedAt,
	}
}
