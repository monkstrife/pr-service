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

type ReassignRequest struct {
	PullRequestID string `json:"pull_request_id"`
	OldUserID     string `json:"old_user_id"` 
}

type ReassignResponse struct {
	PR         PRResponse `json:"pr"`
	ReplacedBy string     `json:"replaced_by"`
}

// Handler

// POST /pullRequest/reassign
func Reassign(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.pullrequest.reassign"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req ReassignRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode request body", sl.Err(err))

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("invalid request body"))

			return
		}

		if req.PullRequestID == "" || req.OldUserID == "" {
			log.Warn("missing required fields",
				slog.String("pull_request_id", req.PullRequestID),
				slog.String("old_user_id", req.OldUserID),
			)

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("pull_request_id and old_user_id are required"))

			return
		}

		pr, replacedBy, err := repo.ReassignReviewer(req.PullRequestID, req.OldUserID)
		if err != nil {
			switch {
			case errors.Is(err, storage.ErrNotFound):
				log.Info("pr or user not found for reassign",
					slog.String("pull_request_id", req.PullRequestID),
					slog.String("old_user_id", req.OldUserID),
				)

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return

			case errors.Is(err, storage.ErrPRMerged):
				log.Info("attempt to reassign on merged PR",
					slog.String("pull_request_id", req.PullRequestID),
				)

				render.Status(r, http.StatusConflict)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "PR_MERGED",
						Message: "cannot reassign on merged PR",
					},
				})

				return

			case errors.Is(err, storage.ErrNotAssigned):
				log.Info("user is not assigned reviewer",
					slog.String("pull_request_id", req.PullRequestID),
					slog.String("old_user_id", req.OldUserID),
				)

				render.Status(r, http.StatusConflict)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_ASSIGNED",
						Message: "reviewer is not assigned to this PR",
					},
				})

				return

			case errors.Is(err, storage.ErrNoCandidate):
				log.Info("no active replacement candidate",
					slog.String("pull_request_id", req.PullRequestID),
					slog.String("old_user_id", req.OldUserID),
				)

				render.Status(r, http.StatusConflict)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NO_CANDIDATE",
						Message: "no active replacement candidate in team",
					},
				})

				return

			default:
				log.Error("failed to reassign reviewer", sl.Err(err))

				render.Status(r, http.StatusInternalServerError)
				render.JSON(w, r, resp.Error("internal error"))

				return
			}
		}

		res := ReassignResponse{
			PR:         mapPullRequestToResponse(pr),
			ReplacedBy: replacedBy,
		}

		log.Info("reviewer reassigned",
			slog.String("pull_request_id", pr.ID),
			slog.String("replaced_by", replacedBy),
		)

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}
