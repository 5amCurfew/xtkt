package xtkt

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/5amCurfew/xtkt/lib"
	xtkt "github.com/5amCurfew/xtkt/pkg"
	"github.com/spf13/cobra"
)

var version = "0.0.1"

var rootCmd = &cobra.Command{
	Use:     "xtkt <PATH_TO_CONFIG_JSON>",
	Version: version,
	Short:   "xtkt - data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a RESTful API or database to pipe to any target that meets the Singer.io specification`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var c lib.Config

		config, readConfigError := os.ReadFile(args[0])
		if readConfigError != nil {
			panic(fmt.Sprintf("Failed to read %s file", args[0]))
		}

		configError := xtkt.ValidateJSONConfig(config)
		if configError == nil {
			jsonError := json.Unmarshal(config, &c)
			if jsonError != nil {
				panic(fmt.Sprintf("Failed to read %s file as json", args[0]))
			}
		} else {
			panic(fmt.Sprintf("Config validation failed: %e", configError))
		}

		xtkt.ParseResponse(c)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	}
}
