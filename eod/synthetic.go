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
package eod

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func BuildSyntheticHistory(asset *SyntheticAsset) error {
	log.Info().Msg("adjust all tickers close price")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return err
	}
	defer conn.Close(ctx)

	// ensure that an asset exists in the database
	sql := `INSERT INTO assets ("ticker", "composite_figi", "active", "name", "asset_type", "listed_utc") VALUES ($1, $2, 't', $3, 'Synthetic History', $4) ON CONFLICT ON CONSTRAINT assets_pkey DO UPDATE SET name = EXCLUDED.name, listed_utc = EXCLUDED.listed_utc`
	if _, err := conn.Exec(ctx, sql, asset.Symbol, asset.FIGI, asset.Name, asset.StartDate); err != nil {
		log.Error().Err(err).Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.FIGI).Msg("could not update asset record in database")
		return err
	}

	// load recent eod quotes for asset

	// save to database
	sql = `INSERT INTO eod ("ticker", "composite_figi", "event_date", "close", "adj_close") VALUES ($1, $2, $3, $4, $5) ON CONFLICT ON CONSTRAINT eod_pkey DO UPDATE SET event_date = EXCLUDED.event_date, close = EXCLUDED.close, adj_close = EXCLUDED.adj_close`
	if _, err := conn.Exec(ctx, sql, asset.Symbol, asset.FIGI, asset.Name, asset.StartDate); err != nil {
		log.Error().Err(err).Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.FIGI).Msg("could not update asset record in database")
		return err
	}

	return nil
}
