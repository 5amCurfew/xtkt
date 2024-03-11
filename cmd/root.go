package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	lib "github.com/5amCurfew/xtkt/lib"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var version = "0.1.9"
var saveSchema bool
var defaultConcurrency int = 1000

func Execute() {
	rootCmd.Flags().BoolVar(&saveSchema, "save-schema", false, "save the schema to a file after extraction")
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

var rootCmd = &cobra.Command{
	Use:     "xtkt [PATH_TO_CONFIG_JSON]",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a RESTful API, database or files to pipe to any target that meets the Singer.io specification`,
	Args:    cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		log.SetFormatter(&log.JSONFormatter{})

		var cfgPath string
		if len(args) == 0 {
			// If no argument provided, look for config.json in the current directory
			log.Info("no config json path provided, defaulting to config.json")
			cfgPath = "config.json"
		} else {
			cfgPath = args[0]
		}

		cfg, cfgError := parseConfigJSON(cfgPath)
		if cfgError != nil {
			log.WithFields(log.Fields{"Error": fmt.Errorf("%w", cfgError)}).Fatalln("failed to parse config JSON - does it exist and is it valid?")
			return fmt.Errorf("error parsing config JSON")
		}
		lib.ParsedConfig = cfg

		if extractError := extract(saveSchema); extractError != nil {
			log.WithFields(log.Fields{"Error": fmt.Errorf("%w", extractError)}).Fatalln("failed to extract records")
			return fmt.Errorf("failed to extract records")
		}
		return nil
	},
}

func parseConfigJSON(filePath string) (lib.Config, error) {
	var cfg lib.Config

	config, readConfigError := os.ReadFile(filePath)
	if readConfigError != nil {
		return cfg, fmt.Errorf("error parseConfigJson reading config.json: %w", readConfigError)
	}

	if jsonError := json.Unmarshal(config, &cfg); jsonError != nil {
		return cfg, fmt.Errorf("error parseConfigJson unmarshlling config.json: %w", jsonError)
	}

	// Check if MaxConcurrency field is nil, set it to the default value
	if cfg.MaxConcurrency == nil {
		cfg.MaxConcurrency = &defaultConcurrency
	}

	return cfg, nil
}
