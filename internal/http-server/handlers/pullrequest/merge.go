package pullrequest

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

type MergeRequest struct {
	PullRequestID string `json:"pull_request_id"`
}

type MergeResponse struct {
	PR PRResponse `json:"pr"`
}

// Handler

// POST /pullRequest/merge
func Merge(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.pullrequest.merge"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req MergeRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode request body", sl.Err(err))

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("invalid request body"))

			return
		}

		if req.PullRequestID == "" {
			log.Warn("pull_request_id is empty")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("pull_request_id is required"))

			return
		}

		pr, err := repo.MergePullRequest(req.PullRequestID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				log.Info("pull request not found", slog.String("pull_request_id", req.PullRequestID))

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return
			}

			log.Error("failed to merge pull request", sl.Err(err))

			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		res := MergeResponse{
			PR: mapPullRequestToResponse(pr),
		}

		log.Info("pull request merged",
			slog.String("pull_request_id", pr.ID),
			slog.String("status", pr.Status),
		)

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}