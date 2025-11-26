package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/5amCurfew/xtkt/cmd"
	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var version = "0.7.1"
var discover bool = false
var refresh bool = false

func main() {
	Execute()
}

func Execute() {
	rootCmd.Flags().BoolVarP(&discover, "discover", "d", false, "run the tap in discovery mode, creating the catalog")
	rootCmd.Flags().BoolVarP(&refresh, "refresh", "r", false, "extract all records (full refresh) rather than only new or modified records (incremental, default)")

	if err := rootCmd.Execute(); err != nil {
		log.WithFields(log.Fields{"Error": err}).Fatalln("error using xtkt")
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

var rootCmd = &cobra.Command{
	Use:     "xtkt [PATH_TO_CONFIG_JSON]",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from RESTful APIs, CSVs, and JSONL files to pipe to any target that meets the Singer.io specification.`,
	Args:    cobra.MaximumNArgs(1),
	RunE: func(command *cobra.Command, args []string) error {
		log.SetFormatter(&log.JSONFormatter{})

		// Default to config.json if no path is provided
		cfgPath := "config.json"
		if len(args) > 0 {
			cfgPath = args[0]
		} else {
			log.Info("no config JSON path provided, defaulting to config.json")
		}

		if err := readConfig(cfgPath); err != nil {
			log.WithFields(log.Fields{"Error": err}).Fatalln("Failed to parse config JSON")
			return fmt.Errorf("error parsing config JSON: %w", err)
		}

		models.STREAM_NAME = models.Config.StreamName

		if err := cmd.Extract(discover, refresh); err != nil {
			log.WithFields(log.Fields{"Error": err}).Fatalln("Failed to extract records")
			return fmt.Errorf("failed to extract records: %w", err)
		}

		return nil
	},
}

func readConfig(filePath string) error {

	config, readConfigError := os.ReadFile(filePath)
	if readConfigError != nil {
		return fmt.Errorf("error parseConfigJson reading config.json: %w", readConfigError)
	}

	if jsonError := json.Unmarshal(config, &models.Config); jsonError != nil {
		return fmt.Errorf("error parseConfigJson unmarshlling config.json: %w", jsonError)
	}

	return nil
}
