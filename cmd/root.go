package xtkt

import (
	"fmt"
	"os"

	xtkt "github.com/5amCurfew/xtkt/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var version = "0.0.791"

var rootCmd = &cobra.Command{
	Use:     "xtkt <PATH_TO_CONFIG_JSON>",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a RESTful API, database or files to pipe to any target that meets the Singer.io specification`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFormatter(&log.JSONFormatter{})

		cfg, cfgError := xtkt.ParseConfigJSON(args[0])
		if cfgError != nil {
			log.WithFields(
				log.Fields{
					"Error": fmt.Errorf("%w", cfgError),
				},
			).Fatalln("failed to parse config JSON")
		}

		if *cfg.SourceType == "listen" {
			xtkt.Listen(cfg)
		} else {
			extractError := xtkt.Extract(cfg)
			if extractError != nil {
				log.WithFields(
					log.Fields{
						"Error": fmt.Errorf("%w", extractError),
					},
				).Fatalln("failed to extract records")
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
