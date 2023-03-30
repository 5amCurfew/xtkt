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
	Use:     "xtkt <CONNECTION_ID>",
	Version: version,
	Short:   "xtkt - REST API data extraction CLI",
	Long:    `xtkt is a command line interface to extract data from a REST API using the Singer Specification`,
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {

		var c util.Config

		config, _ := os.ReadFile("config.json")
		_ = json.Unmarshal(config, &c)

		// https://rickandmortyapi.com/api/character/2, "", "id", "created"
		// https://rickandmortyapi.com/api/character/, "results", "id", "created"
		// https://cat-fact.herokuapp.com/facts, "", "_id", "updatedAt"

		xtkt.ParseResponse(c)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	}
}
