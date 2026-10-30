package prep

import (
	"net/http"
	"strconv"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *prepservice.Service
	parser  *Parser
}

func NewHandler(service *prepservice.Service) *Handler {
	return &Handler{
		service: service,
		parser:  NewParser(),
	}
}

func (h *Handler) Prep(c *gin.Context) {
	runID := c.PostForm("run_id")
	batchID := c.PostForm("batch_id")
	attemptStr := c.PostForm("attempt")

	if runID == "" || batchID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "run_id and batch_id are required"})
		return
	}

	attempt := 1
	if attemptStr != "" {
		a, err := strconv.Atoi(attemptStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid attempt"})
			return
		}
		attempt = a
	}

	file, err := c.FormFile("csv_file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "csv_file is required"})
		return
	}

	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open file"})
		return
	}
	defer f.Close()

	deals, rejected, err := h.parser.Parse(f)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req := models.PrepRequest{
		RunID:   runID,
		BatchID: batchID,
		Attempt: attempt,
	}

	resp := h.service.Prep(c.Request.Context(), req, deals, rejected)

	c.JSON(http.StatusOK, resp)
}
