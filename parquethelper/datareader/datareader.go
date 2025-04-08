package datareader

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	maxOpenConns = 10
	maxIdleConns = 5
)

// sample data reading from parquet
func ReadData() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("connection open error: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	outputDir := "scripttables"
	parquetFile1 := fmt.Sprintf("%s/%s.parquet", outputDir, "NRL1MeasurementInfo")
	parquetFile2 := fmt.Sprintf("%s/%s.parquet", outputDir, "LTE_CellSearchAndCellCoverage")

	query := fmt.Sprintf(`SELECT t1.Time, t1.nr_primary_serving_pci, t2.lte_primary_serving_pci
FROM "%s" t1
JOIN "%s" t2
  ON t1.Time = t2.Time where t1.nr_primary_serving_pci is not null or t2.lte_primary_serving_pci is not null`, parquetFile1, parquetFile2)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("parquet file reading failed: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("columns fetch error: %v", err)
	}
	fmt.Println("Columns:", columns)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			log.Printf("row scanning failed: %v", err)
			continue
		}

		fmt.Println(values)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("row iterating failed: %v", err)
	}
}
