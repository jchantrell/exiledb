package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/database"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query the SQLite database directly from command line",
	Long: `Query allows you to execute SQL queries against the extracted data,
list available tables, or show table schemas.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		listTables, err := cmd.Flags().GetBool("tables")
		if err != nil {
			return fmt.Errorf("failed to get tables flag: %w", err)
		}
		schemaTable, err := cmd.Flags().GetString("schema")
		if err != nil {
			return fmt.Errorf("failed to get schema flag: %w", err)
		}

		slog.Info("Query parameters",
			"database", cfg.Database,
			"list-tables", listTables,
			"schema", schemaTable)

		// Open database connection
		dbOptions := database.DefaultDatabaseOptions(cfg.Database)

		db, err := database.NewDatabase(dbOptions)
		if err != nil {
			return fmt.Errorf("opening database: %w", err)
		}
		defer db.Close()

		// Handle --tables flag
		if listTables {
			slog.Debug("Listing available tables")

			// Simple query to list all tables
			query := `
				SELECT name FROM sqlite_master 
				WHERE type = 'table' AND name NOT LIKE '\_%' 
				ORDER BY name
			`

			rows, err := db.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("listing tables: %w", err)
			}
			defer rows.Close()

			fmt.Println("Available tables:")
			for rows.Next() {
				var tableName string
				if err := rows.Scan(&tableName); err != nil {
					return fmt.Errorf("scanning table name: %w", err)
				}
				fmt.Printf("  %s\n", tableName)
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterating table names: %w", err)
			}

			return nil
		}

		// Handle --schema flag
		if schemaTable != "" {
			slog.Debug("Getting table schema", "table", schemaTable)

			// Query to get table schema
			query := `PRAGMA table_info(` + schemaTable + `)`

			rows, err := db.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("getting schema for table %s: %w", schemaTable, err)
			}
			defer rows.Close()

			fmt.Printf("Schema for table '%s':\n", schemaTable)
			fmt.Printf("%-20s %-15s %-10s %-10s %-20s %-5s\n",
				"Column", "Type", "NotNull", "Default", "Primary", "AutoInc")
			fmt.Println(strings.Repeat("-", 80))

			for rows.Next() {
				var cid int
				var name, dataType string
				var notNull int
				var defaultValue, primaryKey interface{}

				if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
					return fmt.Errorf("scanning schema row: %w", err)
				}

				defaultStr := "NULL"
				if defaultValue != nil {
					defaultStr = fmt.Sprintf("%v", defaultValue)
				}

				primaryStr := "NO"
				if primaryKey != nil && fmt.Sprintf("%v", primaryKey) != "0" {
					primaryStr = "YES"
				}

				fmt.Printf("%-20s %-15s %-10s %-10s %-20s %-5s\n",
					name, dataType,
					map[int]string{0: "NO", 1: "YES"}[notNull],
					defaultStr, primaryStr, "NO")
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterating schema: %w", err)
			}

			return nil
		}

		// Handle SQL query execution
		if len(args) > 0 {
			query := args[0]
			slog.Debug("Executing SQL query", "query", query)

			// Execute query
			rows, err := db.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("executing query: %w", err)
			}
			defer rows.Close()

			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("getting column names: %w", err)
			}

			// Print column headers
			for i, col := range columns {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(col)
			}
			fmt.Println()

			// Print separator
			for i, col := range columns {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(strings.Repeat("-", len(col)))
			}
			fmt.Println()

			// Print rows
			for rows.Next() {
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("scanning row: %w", err)
				}

				for i, val := range values {
					if i > 0 {
						fmt.Print("\t")
					}
					if val != nil {
						fmt.Print(val)
					} else {
						fmt.Print("NULL")
					}
				}
				fmt.Println()
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterating rows: %w", err)
			}

			return nil
		}

		return fmt.Errorf("no query provided, use --tables to list tables or --schema <table> to show schema")
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().Bool("tables", false, "List available tables")
	queryCmd.Flags().String("schema", "", "Show schema for specified table")
}
