package main

import (
	"fmt"
	"os"

	"github.com/5amCurfew/xtkt/cmd"
	"github.com/5amCurfew/xtkt/models"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var version = "0.8.5"
var discover bool = false
var refresh bool = false

func main() {
	Execute()
}

func Execute() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	rootCmd.Flags().BoolVarP(&discover, "discover", "d", false, "run the tap in discovery mode, creating the catalog")
	rootCmd.Flags().BoolVarP(&refresh, "refresh", "r", false, "extract all records (full refresh) rather than only new or modified records (incremental, default)")

	if err := rootCmd.Execute(); err != nil {
		log.WithField("error", err).Fatal("command execution failed")
	}
}

var rootCmd = &cobra.Command{
	Use:     "xtkt [PATH_TO_CONFIG_JSON]",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from RESTful APIs, CSVs, and JSONL files to pipe to any target that meets the Singer.io specification.`,
	Args:    cobra.MaximumNArgs(1),
	RunE: func(command *cobra.Command, args []string) error {
		// Default to config.json if no path is provided
		cfgPath := "config.json"
		if len(args) > 0 {
			cfgPath = args[0]
		} else {
			log.WithField("config_path", cfgPath).Info("no config path provided; using default")
		}

		if err := models.Config.Create(cfgPath); err != nil {
			return fmt.Errorf("error parsing config JSON: %w", err)
		}

		models.STREAM_NAME = models.Config.StreamName

		var err error
		if discover {
			err = cmd.Discover(refresh)
		} else {
			err = cmd.Extract(refresh)
		}

		if err != nil {
			return fmt.Errorf("command run failed: %w", err)
		}

		return nil
	},
}
