package batch

import "github.com/TzimmesTeam/data-prep/internal/domain/models"

func Partition(deals []models.Deal, size int) [][]models.Deal {
	if size <= 0 || len(deals) == 0 {
		return [][]models.Deal{deals}
	}

	var result [][]models.Deal
	for i := 0; i < len(deals); i += size {
		end := i + size
		if end > len(deals) {
			end = len(deals)
		}
		result = append(result, deals[i:end])
	}
	return result
}
