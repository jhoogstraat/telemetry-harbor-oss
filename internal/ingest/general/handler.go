package general

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go-ingest-service/internal/cache"
	"go-ingest-service/internal/config"
	"go-ingest-service/internal/models"
	"go-ingest-service/internal/utils"

	"github.com/gofiber/fiber/v2"
)

// IngestData handles single OR mixed data point ingestion.
func IngestData(c *fiber.Ctx) error {
	// 1. Parse Generic Map
	var rawMap map[string]interface{}
	if err := c.BodyParser(&rawMap); err != nil {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "Invalid request body",
			Details: "Could not parse JSON: " + err.Error(),
		})
	}

	// 2. Explode
	batch, err := ExplodePayload(rawMap)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "Validation Error",
			Details: err.Error(),
		})
	}

	if len(batch) == 0 {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "No valid data found",
			Details: "Payload contained no valid 'value' or numeric metrics.",
		})
	}

	// 3. Process & Queue
	return processAndQueue(c, batch)
}

// IngestBatchData handles an array of maps (flexible batch).
func IngestBatchData(c *fiber.Ctx) error {
	// 1. Parse Slice of Maps
	var rawBatch []map[string]interface{}
	if err := c.BodyParser(&rawBatch); err != nil {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "Invalid request body",
			Details: "Expected a JSON array: " + err.Error(),
		})
	}

	if len(rawBatch) == 0 {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "Invalid request body",
			Details: "Batch cannot be empty.",
		})
	}

	// 2. Explode Everything
	var finalBatch []SensorData
	now := time.Now().UTC()

	for i, item := range rawBatch {
		explodedPoints, err := ExplodePayload(item)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(models.APIError{
				Message: "Batch Processing Error",
				Details: fmt.Sprintf("Error in item index %d: %v", i, err),
			})
		}

		// Ensure time
		for j := range explodedPoints {
			if explodedPoints[j].Time.IsZero() {
				explodedPoints[j].Time = now
			}
		}
		finalBatch = append(finalBatch, explodedPoints...)
	}

	// 3. Process & Queue
	return processAndQueue(c, finalBatch)
}

// processAndQueue centralizes Validation and Redis pushing.
func processAndQueue(c *fiber.Ctx, batch []SensorData) error {
	// A. Validate
	if validationErrors := utils.ValidateBatch(batch); len(validationErrors) > 0 {
		return c.Status(http.StatusBadRequest).JSON(models.APIError{
			Message: "Validation Error",
			Details: validationErrors,
		})
	}

	// B. Queue
	queuedData := models.QueuedData{
		RetryCount: 0,
		Type:       "general",
		Data:       batch,
	}

	dataJSON, err := json.Marshal(queuedData)
	if err != nil {
		return fiber.NewError(http.StatusInternalServerError, "Failed to prepare data")
	}

	if err := cache.RedisClient.RPush(c.Context(), config.AppConfig.IngestQueueName, dataJSON).Err(); err != nil {
		return fiber.NewError(http.StatusInternalServerError, "Failed to queue data")
	}

	return c.Status(http.StatusOK).JSON(fiber.Map{
		"status":  "Data received and queued",
		"ship_id": batch[0].ShipID,
		"count":   len(batch),
	})
}
