/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/pelletier/go-toml/v2"
	"github.com/penny-vault/eod-maintenance/eod"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var print bool
var saveDB bool

// syntheticCmd represents the synthetic command
var syntheticCmd = &cobra.Command{
	Use:   "synthetic",
	Short: "Generate synthetic indexes based on spliced data",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
		if err != nil {
			log.Error().Err(err).Msg("could not connect to database")
			os.Exit(1)
		}
		defer conn.Close(ctx)

		assets := make(map[string]*eod.SyntheticAsset)
		doc, err := ioutil.ReadFile(args[0])
		if err != nil {
			log.Error().Err(err).Str("FileName", args[0]).Msg("could not read input file")
			os.Exit(1)
		}
		if err := toml.Unmarshal(doc, &assets); err != nil {
			log.Error().Err(err).Msg("error parsing toml")
			os.Exit(1)
		}

		for _, asset := range assets {
			// load recent eod quotes for asset
			history := eod.LoadEodHistory(ctx, conn, asset)
			quotes, err := eod.BuildSyntheticHistory(ctx, asset, history)
			if err != nil {
				continue
			}

			if print {
				eod.PrintEod(quotes)
			}

			if saveDB {
				eod.UpdateSyntheticHistory(ctx, conn, asset, quotes)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(syntheticCmd)

	syntheticCmd.Flags().BoolVarP(&print, "print", "p", false, "Print EOD quotes to the screen")
	syntheticCmd.Flags().BoolVarP(&saveDB, "save", "s", false, "Save EOD quotes to the database")
}
