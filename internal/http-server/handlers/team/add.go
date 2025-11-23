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

type AddRequest struct {
	TeamName string `json:"team_name" validate:"required"`
	Members  []struct {
		UserID   string `json:"user_id" validate:"required"`
		Username string `json:"username" validate:"required"`
		IsActive bool   `json:"is_active"`
	} `json:"members"`
}

type AddResponse struct {
	Team struct {
		TeamName string `json:"team_name"`
		Members  []struct {
			UserID   string `json:"user_id"`
			Username string `json:"username"`
			IsActive bool   `json:"is_active"`
		} `json:"members"`
	} `json:"team"`
}

func Add(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.team.add"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req AddRequest
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			log.Error("failed to decode body", sl.Err(err))
			render.JSON(w, r, resp.Error("invalid body"))
			return
		}

		members := make([]storage.TeamMember, 0, len(req.Members))
		for _, m := range req.Members {
			members = append(members, storage.TeamMember{
				UserID:   m.UserID,
				Username: m.Username,
				IsActive: m.IsActive,
			})
		}

		team, err := repo.CreateTeam(req.TeamName, members)
		if err != nil {
			if errors.Is(err, storage.ErrTeamExists) {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, map[string]any{
					"error": map[string]any{
						"code":    "TEAM_EXISTS",
						"message": "team_name already exists",
					},
				})
				return
			}
			log.Error("failed to create team", sl.Err(err))
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))
			return
		}

		var res AddResponse
		res.Team.TeamName = team.TeamName
		for _, m := range team.Members {
			res.Team.Members = append(res.Team.Members, struct {
				UserID   string `json:"user_id"`
				Username string `json:"username"`
				IsActive bool   `json:"is_active"`
			}{
				UserID:   m.UserID,
				Username: m.Username,
				IsActive: m.IsActive,
			})
		}

		render.Status(r, http.StatusCreated)
		render.JSON(w, r, res)
	}
}
