package schemacreator

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"example.com/kpi-processing/models"
)

// schema creator using meta data
func CreateSchema(db *sql.DB, kpiData *[]models.KpiInfo) {
	outputDir := "scripttables"

	if _, err := os.Stat(outputDir); err == nil {
		if err := os.RemoveAll(outputDir); err != nil {
			log.Fatalf("could not remove scripttables directory: %v", err)
		}
	}

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		log.Fatalf("could not create scripttables directory: %v", err)
	}

	groupedData := groupByScriptTableName(kpiData)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, runtime.NumCPU())

	for tableName, kpis := range groupedData {
		wg.Add(1)
		go func(tn string, k []models.KpiInfo) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			createTableSQL := generateCreateTableSQL(tn, k)
			if err := executeWithRetry(db, createTableSQL); err != nil {
				log.Printf("table creation failed %s: %v", tn, err)
				return
			}

			parquetFile := fmt.Sprintf("%s/%s.parquet", outputDir, tn)
			exportSQL := fmt.Sprintf(`COPY %s TO '%s' (FORMAT 'parquet')`, tn, parquetFile)
			if err := executeWithRetry(db, exportSQL); err != nil {
				log.Printf("exporting parquet failed %s: %v", tn, err)
				return
			}

			log.Printf("parquet '%s' created successfully!", parquetFile)
		}(tableName, kpis)
	}

	wg.Wait()
}

func groupByScriptTableName(kpis *[]models.KpiInfo) map[string][]models.KpiInfo {
	grouped := make(map[string][]models.KpiInfo)
	for _, kpi := range *kpis {
		grouped[kpi.ScriptTableName] = append(grouped[kpi.ScriptTableName], kpi)
	}
	return grouped
}

func generateCreateTableSQL(tableName string, kpis []models.KpiInfo) string {
	sql := fmt.Sprintf("CREATE TABLE %s (Time TIMESTAMP, ", tableName)
	for i, kpi := range kpis {
		sql += fmt.Sprintf("%s %s", kpi.KpiName, kpi.DataType)
		if i < len(kpis)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

const maxRetries = 3

func executeWithRetry(db *sql.DB, query string) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		_, err = db.Exec(query)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i+1))
	}
	return err
}
