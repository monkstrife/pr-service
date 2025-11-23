package team

import (
	"errors"
	"net/http"

	"log/slog"

	resp "pr-service/internal/lib/api/response"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
)

type GetTeamMember struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	IsActive bool   `json:"is_active"`
}

type GetResponse struct {
	TeamName string          `json:"team_name"`
	Members  []GetTeamMember `json:"members"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// GET /team/get?team_name=...
func Get(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.team.get"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		teamName := r.URL.Query().Get("team_name")
		if teamName == "" {
			log.Warn("team_name query param is empty")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, resp.Error("team_name is required"))

			return
		}

		team, err := repo.GetTeam(teamName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				log.Info("team not found", slog.String("team_name", teamName))

				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrorResponse{
					Error: ErrorBody{
						Code:    "NOT_FOUND",
						Message: "resource not found",
					},
				})

				return
			}

			log.Error("failed to get team", sl.Err(err))

			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		res := GetResponse{
			TeamName: team.TeamName,
			Members:  make([]GetTeamMember, 0, len(team.Members)),
		}

		for _, m := range team.Members {
			res.Members = append(res.Members, GetTeamMember{
				UserID:   m.UserID,
				Username: m.Username,
				IsActive: m.IsActive,
			})
		}

		log.Info("team fetched", slog.String("team_name", team.TeamName))

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}
