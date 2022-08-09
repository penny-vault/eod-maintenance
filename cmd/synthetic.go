// Copyright 2022
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

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

		abspath, err := filepath.Abs(args[0])
		if err != nil {
			log.Error().Err(err).Str("FileName", args[0]).Msg("could not get abs path for input file")
			os.Exit(1)
		}
		dirpath := filepath.Dir(abspath)
		if err := syscall.Chdir(dirpath); err != nil {
			log.Error().Err(err).Str("DirPath", dirpath).Msg("could not change working directory")
			os.Exit(1)
		}
		log.Info().Str("WorkingDir", dirpath).Msg("set working dir")

		doc, err := os.ReadFile(args[0])
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
			log.Info().Str("Asset.Symbol", asset.Symbol).Str("Asset.Name", asset.Name).Msg("building synthetic history for specified asset")
			quotes, err := eod.BuildSyntheticHistory(ctx, asset, history)
			if err != nil {
				continue
			}

			if print {
				eod.PrintEod(quotes)
			}

			if saveDB {
				if err := eod.UpdateSyntheticHistory(ctx, conn, asset, quotes); err != nil {
					os.Exit(1)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(syntheticCmd)

	syntheticCmd.Flags().BoolVarP(&print, "print", "p", false, "Print EOD quotes to the screen")
	syntheticCmd.Flags().BoolVarP(&saveDB, "save", "s", false, "Save EOD quotes to the database")
}
