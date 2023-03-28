package xtkt

import (
	"fmt"
	"os"

	xtkt "github.com/5amCurfew/xtkt/pkg"
	"github.com/spf13/cobra"
)

var version = "0.0.1"

var rootCmd = &cobra.Command{
	Use:     "xtkt <PATH_TO_FILE>",
	Version: version,
	Short:   "xtkt - REST API data extraction CLI",
	Long:    `xpln is a command line interface to extract data from a REST API using the Singer Specification`,
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {

		// https://rickandmortyapi.com/api/character/2, "", "id", "created"
		// https://rickandmortyapi.com/api/character/, "results", "id", "created"
		// https://cat-fact.herokuapp.com/facts, "", "_id", "updatedAt"

		xtkt.ParseResponse(
			// url
			"https://rickandmortyapi.com/api/character",
			// responseRecordsPath
			"results",
			// ID
			"id",
			// bookmark
			"created",
		)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error using xtkt: '%s'", err)
		os.Exit(1)
	}
}
