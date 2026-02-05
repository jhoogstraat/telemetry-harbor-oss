package general

import (
	"fmt"
	"time"

	"go-ingest-service/internal/utils"
)

// ExplodePayload converts a flexible map into a slice of strict SensorData.
func ExplodePayload(rawMap map[string]interface{}) ([]SensorData, error) {
	var results []SensorData

	// 1. Extract Common Metadata (ShipID & Time)
	shipID, ok := rawMap["ship_id"].(string)
	if !ok || shipID == "" {
		return nil, fmt.Errorf("missing ship_id")
	}

	finalTime := time.Now().UTC()
	if tStr, ok := rawMap["time"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339, tStr); err == nil {
			finalTime = parsed
		}
	}

	// 2. Handle "Strict" Legacy Metric (cargo_id + value)
	if cID, ok := rawMap["cargo_id"].(string); ok {
		if valInterface, exists := rawMap["value"]; exists {
			if floatVal, ok := utils.ToFloat64(valInterface); ok {
				results = append(results, SensorData{
					Time:    finalTime,
					ShipID:  shipID,
					CargoID: cID,
					Value:   &floatVal,
				})
			}
		}
	}

	// 3. Handle "Flat" Metrics (Iterate everything else)
	for k, v := range rawMap {
		// Skip Reserved Keys
		if k == "ship_id" || k == "time" || k == "cargo_id" || k == "value" || k == "access_token" {
			continue
		}

		if floatVal, ok := utils.ToFloat64(v); ok {
			results = append(results, SensorData{
				Time:    finalTime,
				ShipID:  shipID,
				CargoID: k,
				Value:   &floatVal,
			})
		}
	}

	return results, nil
}
