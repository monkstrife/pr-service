package stats

import (
	"net/http"

	"log/slog"

	resp "pr-service/internal/lib/api/response"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

// DTO ответа для /stats
type GetStatsResponse struct {
	TotalPullRequests       int                         `json:"total_pull_requests"`
	TotalOpenPullRequests   int                         `json:"total_open_pull_requests"`
	TotalMergedPullRequests int                         `json:"total_merged_pull_requests"`
	AssignmentsByReviewer   []ReviewerAssignmentStatDTO `json:"assignments_by_reviewer"`
}

type ReviewerAssignmentStatDTO struct {
	UserID        string `json:"user_id"`
	AssignedCount int    `json:"assigned_count"`
}

// GET /stats
func Get(log *slog.Logger, repo storage.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.stats.get"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		stats, err := repo.GetStats()
		if err != nil {
			log.Error("failed to get stats", sl.Err(err))

			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		res := GetStatsResponse{
			TotalPullRequests:       stats.TotalPullRequests,
			TotalOpenPullRequests:   stats.TotalOpenPullRequests,
			TotalMergedPullRequests: stats.TotalMergedPullRequests,
			AssignmentsByReviewer:   make([]ReviewerAssignmentStatDTO, 0, len(stats.AssignmentsByReviewer)),
		}

		for _, st := range stats.AssignmentsByReviewer {
			res.AssignmentsByReviewer = append(res.AssignmentsByReviewer, ReviewerAssignmentStatDTO{
				UserID:        st.UserID,
				AssignedCount: st.AssignedCount,
			})
		}

		render.Status(r, http.StatusOK)
		render.JSON(w, r, res)
	}
}
