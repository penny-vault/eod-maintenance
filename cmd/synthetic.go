/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"io/ioutil"

	"github.com/pelletier/go-toml/v2"
	"github.com/penny-vault/eod-maintenance/eod"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// syntheticCmd represents the synthetic command
var syntheticCmd = &cobra.Command{
	Use:   "synthetic",
	Short: "Generate synthetic indexes based on spliced data",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		assets := make(map[string]*eod.SyntheticAsset)
		doc, err := ioutil.ReadFile(args[0])
		if err != nil {
			log.Error().Err(err).Str("FileName", args[0]).Msg("could not read input file")
		}
		if err := toml.Unmarshal(doc, &assets); err != nil {
			log.Error().Err(err).Msg("error parsing toml")
		}

		for _, v := range assets {
			eod.BuildSyntheticHistory(v)
		}
	},
}

func init() {
	rootCmd.AddCommand(syntheticCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// syntheticCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// syntheticCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
