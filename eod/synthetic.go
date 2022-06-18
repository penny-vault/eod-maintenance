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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// UpdateSyntheticHistory updates the database with the synthetic asset
func UpdateSyntheticHistory(ctx context.Context, conn *pgx.Conn, asset *SyntheticAsset, history []*Eod) error {
	log.Info().Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.CompositeFigi).Msg("update synthetic history eod prices")

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not start transaction")
		return err
	}

	// ensure that an asset exists in the database
	saveSyntheticAsset(ctx, tx, asset)

	// save to database
	saveSyntheticEod(ctx, tx, history)

	err = tx.Commit(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not commit transaction to database")
		return err
	}

	return nil
}

// BuildSyntheticHistory iterates over all the components of a synthetic asset and calculates
func BuildSyntheticHistory(ctx context.Context, asset *SyntheticAsset, history []*Eod) ([]*Eod, error) {
	newHistory := make([]*Eod, 0)

	// set starting value of synthetic asset
	quote := &Eod{
		EventDate:     asset.StartDate,
		Ticker:        asset.Symbol,
		CompositeFigi: asset.CompositeFigi,
		Close:         10.0,
		AdjClose:      10.0,
	}
	if len(history) > 0 {
		quote = history[0]
	} else {
		log.Info().Str("Ticker", asset.Symbol).Msg("starting synthetic history at 1.0")
		newHistory = append(newHistory, quote)
	}

	// read components, calculate percent change, add eod quotes
	for _, component := range asset.Components {
		if !component.End.Equal(time.Time{}) && component.End.Before(quote.EventDate) {
			// component has already been incorporated in EOD quotes
			continue
		}
		pctChange, err := getComponentPctChange(ctx, component)
		if err != nil {
			return newHistory, err
		}
		for _, pct := range pctChange {
			if pct.Date.Before(quote.EventDate) || pct.Date.Equal(quote.EventDate) {
				continue
			}
			if !component.End.Equal(time.Time{}) && pct.Date.After(component.End) {
				log.Info().Time("Date", pct.Date).Time("PctDate", pct.Date).Str("Name", component.Name).Msg("Component ended")
				break
			}
			close := quote.Close * pct.Percent
			quote = &Eod{
				EventDate:     pct.Date,
				Ticker:        asset.Symbol,
				CompositeFigi: asset.CompositeFigi,
				Close:         close,
				AdjClose:      close,
			}
			newHistory = append(newHistory, quote)
		}
	}

	return newHistory, nil
}

// getComponentPctChange reads the component
func getComponentPctChange(ctx context.Context, component *SyntheticComponent) ([]*PercentChange, error) {
	pctChange := []*PercentChange{}

	if component.FileName != "" {
		// load from file
		fh, err := os.OpenFile(component.FileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			log.Error().Err(err).Str("FileName", component.FileName).Msg("could not open component file")
			return pctChange, err
		}
		defer fh.Close()

		history := []*Eod{}

		if err := gocsv.UnmarshalFile(fh, &history); err != nil {
			log.Error().Err(err).Str("FileName", component.FileName).Msg("could not open component file")
			return pctChange, err
		}

		last := 0.0
		for _, quote := range history {
			if quote.EventDate, err = time.Parse("2006-01-02", quote.EventDateStr); err != nil {
				log.Error().Err(err).Str("DateString", quote.EventDateStr).Msg("could not parse event date")
			}
			pct := quote.AdjClose / last
			pctChange = append(pctChange, &PercentChange{
				Date:    quote.EventDate,
				Percent: pct,
			})
			last = quote.AdjClose
		}
	} else if component.CompositeFigi != "" {
		// load from database
		conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
		if err != nil {
			log.Error().Err(err).Msg("could not connect to database")
			return pctChange, err
		}
		defer conn.Close(ctx)

		rows, err := conn.Query(ctx, `SELECT event_date, (adj_close / (LAG (adj_close,1) OVER (ORDER BY event_date ASC)))::double precision AS pct_change FROM eod WHERE composite_figi = $1 ORDER BY event_date ASC`, component.CompositeFigi)
		if err != nil {
			log.Error().Err(err).Msg("could not retreive price history from db")
			return pctChange, err
		}
		for rows.Next() {
			pct := &PercentChange{}
			var dbPercentVal pgtype.Float8
			err = rows.Scan(&pct.Date, &dbPercentVal)
			if err != nil {
				log.Error().Err(err).Msg("could not scan result into PercentChange from db")
				return pctChange, err
			}
			if dbPercentVal.Status != pgtype.Null {
				pct.Percent = dbPercentVal.Float
				pctChange = append(pctChange, pct)
			}
		}
	} else {
		err := errors.New("one of CompositeFigi or FileName must be set on a component")
		log.Error().Err(err).Msg("asset component is mis-specified")
		return pctChange, err
	}

	return pctChange, nil
}

// loadEodHistory reads recent EOD quotes from eod
func LoadEodHistory(ctx context.Context, conn *pgx.Conn, asset *SyntheticAsset) []*Eod {
	history := make([]*Eod, 0)
	sql := `SELECT event_date, close FROM eod WHERE composite_figi=$1 ORDER BY event_date DESC LIMIT 5`
	rows, err := conn.Query(ctx, sql, asset.CompositeFigi)
	if err != nil {
		log.Error().Err(err).Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.CompositeFigi).Msg("could not query database for asset")
		return history
	}
	for rows.Next() {
		v := &Eod{}
		err := rows.Scan(&v.EventDate, &v.Close)
		if err != nil {
			log.Error().Err(err).Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.CompositeFigi).Msg("could not scan result into eod")
			return history
		}
		history = append(history, v)
	}
	return history
}

// PrintEod prints EOD quotes to the screen
func PrintEod(quotes []*Eod) {
	for _, quote := range quotes {
		fmt.Printf("%s\t%s\t%.5f\n", quote.EventDate.Format("2006-01-02"), quote.Ticker, quote.Close)
	}
}

// saveSyntheticAsset updates the database asset record
func saveSyntheticAsset(ctx context.Context, tx pgx.Tx, asset *SyntheticAsset) error {
	sql := `INSERT INTO assets ("ticker", "composite_figi", "active", "name", "asset_type", "listed_utc") VALUES ($1, $2, 't', $3, 'Synthetic History', $4) ON CONFLICT ON CONSTRAINT assets_pkey DO UPDATE SET name = EXCLUDED.name, listed_utc = EXCLUDED.listed_utc`
	if _, err := tx.Exec(ctx, sql, asset.Symbol, asset.CompositeFigi, asset.Name, asset.StartDate); err != nil {
		log.Error().Err(err).Str("Ticker", asset.Symbol).Str("CompositeFigi", asset.CompositeFigi).Msg("could not update asset record in database")
		tx.Rollback(ctx)
		return err
	}
	return nil
}

// saveSyntheticEod saves EOD quotes to the database
func saveSyntheticEod(ctx context.Context, tx pgx.Tx, quotes []*Eod) error {
	for _, quote := range quotes {
		if _, err := tx.Exec(ctx, `INSERT INTO eod ("event_date", "ticker", "composite_figi", "close") VALUES ($1, $2, $3, $4) ON CONFLICT ON CONSTRAINT eod_pkey DO UPDATE SET close = EXCLUDED.close`, quote.EventDate, quote.Ticker, quote.CompositeFigi, quote.Close); err != nil {
			log.Error().Err(err).Msg("could not save eod quote to database")
			tx.Rollback(ctx)
			return err
		}
	}
	return nil
}
