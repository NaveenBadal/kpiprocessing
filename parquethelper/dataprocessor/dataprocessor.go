package dataprocessor

import (
	"database/sql"
	"log"
	"time"

	"example.com/kpi-processing/models"
	"example.com/kpi-processing/parquethelper/datainserter"
	"example.com/kpi-processing/parquethelper/schemacreator"
)

const (
	BatchSize    = 1000
	maxOpenConns = 10
	maxIdleConns = 5
)

// kpi data processing
// holding the data in batches and then inserting in the db
func ProcessBatches(kpiDataChan chan models.KPIDataArgs, metadataChan chan []models.KpiInfo, done chan struct{}) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	batch := make(map[string][]models.KPIDataArgs)
	processedTables := make(map[string]bool)

	for {
		select {
		case kpiData, ok := <-kpiDataChan:
			if !ok {
				log.Println("kpidata channel closed, inserting remaining data")
				for tableName, data := range batch {
					if len(data) > 0 {
						insertBatch(db, tableName, data)
					}
					if err := datainserter.ExportToParquet(db, tableName); err != nil {
						log.Printf("error in parquet exporting %s: %v", tableName, err)
					}
				}
				close(done)
				return
			}
			batch[kpiData.ScriptTableName] = append(batch[kpiData.ScriptTableName], kpiData)
			processedTables[kpiData.ScriptTableName] = true

			if len(batch[kpiData.ScriptTableName]) >= BatchSize {
				insertBatch(db, kpiData.ScriptTableName, batch[kpiData.ScriptTableName])
				batch[kpiData.ScriptTableName] = nil
			}
			//schema creation using meta data
		case metadata, ok := <-metadataChan:
			if !ok {
				continue
			}
			schemacreator.CreateSchema(db, &metadata)
		}
	}
}

func insertBatch(db *sql.DB, tableName string, batch []models.KPIDataArgs) {
	datainserter.InsertData(db, tableName, batch)
}
