package xtkt

import (
	"encoding/json"
	"fmt"
	"os"

	xtkt "github.com/5amCurfew/xtkt/pkg"
	util "github.com/5amCurfew/xtkt/util"
	"github.com/spf13/cobra"
)

var version = "0.0.1"

var rootCmd = &cobra.Command{
	Use:     "xtkt <PATH_TO_CONFIG_JSON>",
	Version: version,
	Short:   "xtkt - REST API data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a REST API using the Singer.io Specification`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var file = args[0]
		var c util.Config

		config, readConfigError := os.ReadFile(file)
		if readConfigError != nil {
			panic(fmt.Sprintf("Failed to read %s file", file))
		}

		jsonError := json.Unmarshal(config, &c)
		if jsonError != nil {
			panic("xtkt panicing")
		}

		configValidated, validationError := util.ValidateConfig(c)
		if configValidated {
			xtkt.ParseResponse(c)
		} else {
			fmt.Println(validationError)
			panic("xtkt panicing")
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	}
}
