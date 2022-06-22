// Copyright 2022
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/jackc/pgx/v4"
	"github.com/penny-vault/eod-maintenance/eod"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var recent bool
var clean bool

// adjustedCmd represents the adjusted command
var adjustCmd = &cobra.Command{
	Use:   "adjust",
	Short: "Calculate adjusted eod prices",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
		if err != nil {
			log.Error().Err(err).Msg("could not connect to database")
			os.Exit(1)
		}
		defer conn.Close(ctx)

		assets := make([]string, 0)
		if recent {
			rows, err := conn.Query(ctx, `SELECT DISTINCT composite_figi FROM eod WHERE event_date >= now() - interval '2 days' AND (split_factor != 1.0 OR dividend > 0.0)`)
			if err != nil {
				log.Error().Err(err).Msg("could not query database for unique assets")
			}
			for rows.Next() {
				var figi string
				if err := rows.Scan(&figi); err != nil {
					log.Error().Err(err).Msg("could not scan composite_figi into variable")
					os.Exit(1)
				}
				assets = append(assets, figi)
			}
		}

		if clean {
			rows, err := conn.Query(ctx, `SELECT DISTINCT composite_figi FROM eod WHERE adj_close is null;`)
			if err != nil {
				log.Error().Err(err).Msg("could not query database for unique assets")
			}
			for rows.Next() {
				var figi string
				if err := rows.Scan(&figi); err != nil {
					log.Error().Err(err).Msg("could not scan composite_figi into variable")
					os.Exit(1)
				}
				assets = append(assets, figi)
			}
		}

		if !recent && !clean && len(args) == 0 {
			rows, err := conn.Query(ctx, `SELECT DISTINCT composite_figi FROM assets`)
			if err != nil {
				log.Error().Err(err).Msg("could not query database for unique assets")
			}
			for rows.Next() {
				var figi string
				if err := rows.Scan(&figi); err != nil {
					log.Error().Err(err).Msg("could not scan composite_figi into variable")
					os.Exit(1)
				}
				assets = append(assets, figi)
			}
		}

		// convert input arguments to figi's
		for _, inp := range args {
			var figi string
			if err := conn.QueryRow(ctx, `SELECT composite_figi FROM assets WHERE ticker = $1 OR composite_figi = $1 LIMIT 1`, inp).Scan(&figi); err != nil {
				log.Error().Err(err).Str("InputArg", inp).Msg("could not convert input argument to composite figi")
				continue
			}
			assets = append(assets, figi)
		}

		log.Info().Int("NumAssets", len(assets)).Msg("adjusting close prices")
		for _, asset := range assets {
			log.Info().Str("CompositeFigi", asset).Msg("adjusting close price for asset")
			prices, err := eod.AdjustAssetEodPrice(ctx, conn, asset)
			if err != nil {
				log.Error().Err(err).Str("CompositeFigi", asset).Msg("could not adjust asset prices")
				continue
			}
			if err := eod.SaveAdjCloseToDb(ctx, conn, prices); err != nil {
				log.Error().Err(err).Str("CompositeFigi", asset).Msg("could not save adjusted close to db")
				continue
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(adjustCmd)

	adjustCmd.Flags().BoolVarP(&recent, "recent", "r", false, "calculated adjusted price for recently changed eod tickers")
	adjustCmd.Flags().BoolVarP(&clean, "clean", "c", false, "clean assets that have null values in adj_close")
}
