package xtkt

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/5amCurfew/xtkt/lib"
	xtkt "github.com/5amCurfew/xtkt/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var version = "0.0.79"

var rootCmd = &cobra.Command{
	Use:     "xtkt <PATH_TO_CONFIG_JSON>",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a RESTful API or database to pipe to any target that meets the Singer.io specification`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFormatter(&log.JSONFormatter{})
		var cfg lib.Config

		config, readConfigError := os.ReadFile(args[0])
		if readConfigError != nil {
			log.WithFields(
				log.Fields{
					"Error": fmt.Sprintf("%v", readConfigError),
				},
			).Fatalln("Failed to READ CONFIG.JSON")
		}

		configError := xtkt.ValidateJSONConfig(config)
		if configError != nil {
			log.WithFields(
				log.Fields{
					"Error": fmt.Sprintf("%v", configError),
				},
			).Fatalln("Failed to VALIDATE CONFIG.JSON")
		} else {
			jsonError := json.Unmarshal(config, &cfg)
			if jsonError != nil {
				log.WithFields(
					log.Fields{
						"Error": fmt.Sprintf("%v", jsonError),
					},
				).Fatalln("Failed to PARSE CONFIG.JSON")
			}
		}

		if *cfg.SourceType == "listen" {
			xtkt.Listen(cfg)
		} else {
			parseError := xtkt.Extract(cfg)
			if parseError != nil {
				log.WithFields(
					log.Fields{
						"Error": fmt.Sprintf("%v", parseError),
					},
				).Fatalln("Failed to EXTRACT RECORDS")
			}
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	}
}
