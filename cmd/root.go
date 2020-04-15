package cmd

import (
	"github.com/cyliu0/tigen/pkg/db"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"os"
)

var logger *zap.Logger

var rootCmd = &cobra.Command{
	Use:   "tigen",
	Short: "A toolkit to generate test data for TiDB/MySQL",
	Run: func(cmd *cobra.Command, args []string) {
		logger, _ = zap.NewProduction()
		defer logger.Sync()
		c := db.MysqlClient{
			Addr: db.Addr{
				Host: host,
				Port: port,
				User: user,
				Pass: pass,
			},
			Schema: database,
			Logger: logger,
		}
		c.GenTableWithData(table, columnCount, rowCount, threadCount)
		logger.Info("Data generating finished...")
	},
}

var host, user, pass string
var port int
var database, table string
var columnCount, rowCount, threadCount int

func init() {
	rootCmd.PersistentFlags().StringVar(&host, "host", "127.0.0.1", "DB host")
	rootCmd.PersistentFlags().IntVar(&port, "port", 4000, "DB port")
	rootCmd.PersistentFlags().StringVar(&user, "user", "root", "DB username")
	rootCmd.PersistentFlags().StringVar(&pass, "pass", "", "DB password")
	rootCmd.PersistentFlags().StringVar(&database, "database", "test", "Database name")
	rootCmd.PersistentFlags().StringVar(&table, "table", "t", "Table name")
	rootCmd.PersistentFlags().IntVar(&columnCount, "columns", 10, "Column count")
	rootCmd.PersistentFlags().IntVar(&rowCount, "rows", 20000, "Row count")
	rootCmd.PersistentFlags().IntVar(&threadCount, "threads", 10, "Thread count")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		logger.Error("Execute command error", zap.Error(err))
		os.Exit(1)
	}
}
