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

type SetIsActiveRequest struct {
	UserID   string `json:"user_id"`
	IsActive bool   `json:"is_active"`
}

type SetIsActiveResponse struct {
	User SetIsActiveUser `json:"user"`
}

type SetIsActiveUser struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	TeamName string `json:"team_name"`
	IsActive bool   `json:"is_active"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Handler

// POST /users/setIsActive
func SetIsActive(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.users.set_is_active"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req SetIsActiveRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode request body", sl.Err(err))

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("invalid request body"))

			return
		}

		if req.UserID == "" {
			log.Warn("user_id is empty")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("user_id is required"))

			return
		}

		user, err := repo.SetUserIsActive(req.UserID, req.IsActive)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				log.Info("user not found", slog.String("user_id", req.UserID))

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return
			}

			log.Error("failed to set user active flag", sl.Err(err))

			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		res := SetIsActiveResponse{
			User: SetIsActiveUser{
				UserID:   user.UserID,
				Username: user.Username,
				TeamName: user.TeamName,
				IsActive: user.IsActive,
			},
		}

		log.Info("user activity updated",
			slog.String("user_id", user.UserID),
			slog.Bool("is_active", user.IsActive),
		)

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}
