package datainserter

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"example.com/kpi-processing/models"
)

var tableLocks = make(map[string]*sync.Mutex)
var tableLocksMutex sync.Mutex

// creating locks for each table
func getTableLock(tableName string) *sync.Mutex {
	tableLocksMutex.Lock()
	defer tableLocksMutex.Unlock()

	if lock, exists := tableLocks[tableName]; exists {
		return lock
	}

	lock := &sync.Mutex{}
	tableLocks[tableName] = lock
	return lock
}

// inserting data in db
func InsertData(db *sql.DB, tableName string, batch []models.KPIDataArgs) {
	if len(batch) == 0 {
		return
	}

	tableLock := getTableLock(tableName)
	tableLock.Lock()
	defer tableLock.Unlock()

	tx, err := db.Begin()
	if err != nil {
		log.Printf("begin transaction failed: %v", err)
		return
	}
	defer tx.Rollback()

	columnNames, placeholders := prepareInsertStatement(batch[0])
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("preparing insert statement failed: %v", err)
		return
	}
	defer stmt.Close()

	for _, data := range batch {
		values := extractValues(data, columnNames)
		if _, err := stmt.Exec(values...); err != nil {
			log.Printf("inserting data failed: %v", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("commiting transaction failed: %v", err)
		return
	}
}

// exporting table to parquet file
func ExportToParquet(db *sql.DB, tableName string) error {
	tableLock := getTableLock(tableName)
	tableLock.Lock()
	defer tableLock.Unlock()

	startTime := time.Now()

	outputDir := "scripttables"
	parquetFile := fmt.Sprintf("%s/%s.parquet", outputDir, tableName)
	exportSQL := fmt.Sprintf(`COPY %s TO '%s' (FORMAT 'parquet')`, tableName, parquetFile)

	if _, err := db.Exec(exportSQL); err != nil {
		return fmt.Errorf("error exporting %s to Parquet: %v", tableName, err)
	}

	log.Printf("parquet file exported: %s (took %v)",
		parquetFile, time.Since(startTime))
	return nil
}

func prepareInsertStatement(kpiData models.KPIDataArgs) ([]string, []string) {
	var columnNames []string
	var placeholders []string

	columnNames = append(columnNames, "Time")
	placeholders = append(placeholders, "?")

	for kpiName := range kpiData.KPIVals {
		columnName, exists := kpiData.KpiDisplayNameToKpiNameDict[kpiName]
		if exists {
			columnNames = append(columnNames, columnName)
			placeholders = append(placeholders, "?")
		}
	}

	return columnNames, placeholders
}

func extractValues(kpiData models.KPIDataArgs, columnNames []string) []interface{} {
	values := make([]interface{}, len(columnNames))
	values[0] = kpiData.Time.Time

	for i, colName := range columnNames[1:] {
		for kpiName, kpiValue := range kpiData.KPIVals {
			if mappedName, exists := kpiData.KpiDisplayNameToKpiNameDict[kpiName]; exists && mappedName == colName {
				values[i+1] = kpiValue
				break
			}
		}
	}

	return values
}
