package team

import (
	"net/http"

	"log/slog"

	resp "pr-service/internal/lib/api/response"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

type DeactivateUsersRequest struct {
	TeamName string   `json:"team_name" validate:"required"`
	UserIDs  []string `json:"user_ids" validate:"required"` // кого деактивируем
}

type DeactivateUsersResponse struct {
	TeamName           string   `json:"team_name"`
	DeactivatedUserIDs []string `json:"deactivated_user_ids"`
	ReassignedCount    int      `json:"reassigned_reviewers"`
	RemovedCount       int      `json:"removed_reviewers"`
}

func DeactivateUsers(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.team.deactivateUsers"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req DeactivateUsersRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode body", sl.Err(err))
			render.JSON(w, r, resp.Error("invalid body"))
			return
		}

		if req.TeamName == "" || len(req.UserIDs) == 0 {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("team_name and user_ids are required"))
			return
		}

		resBulk, err := repo.BulkDeactivateUsersAndReassign(req.TeamName, req.UserIDs)
		if err != nil {
			if err == storage.ErrNotFound {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, map[string]any{
					"error": map[string]any{
						"code":    "NOT_FOUND",
						"message": "resource not found",
					},
				})
				return
			}

			log.Error("failed bulk deactivate users", sl.Err(err))
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))
			return
		}

		respBody := DeactivateUsersResponse{
			TeamName:           resBulk.TeamName,
			DeactivatedUserIDs: resBulk.DeactivatedUserIDs,
			ReassignedCount:    resBulk.ReassignedCount,
			RemovedCount:       resBulk.RemovedAssignments,
		}

		render.Status(r, http.StatusOK)
		render.JSON(w, r, respBody)
	}
}
