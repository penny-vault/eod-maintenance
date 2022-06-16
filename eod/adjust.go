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
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func AdjustAllTickers() error {
	log.Info().Msg("adjust all tickers close price")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return err
	}
	defer conn.Close(ctx)

	conn2, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not create 2nd connection to database")
		return err
	}
	defer conn2.Close(ctx)

	tx, err := conn2.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not start transaction")
		return err
	}

	var currTicker string = ""
	var adjustFactor float64 = 1.0
	rows, err := conn.Query(ctx, "SELECT event_date, ticker, close, dividend, split_factor FROM eod ORDER BY ticker, event_date DESC")
	if err != nil {
		log.Error().Err(err).Msg("SELECT query error")
		return err
	}
	for rows.Next() {
		var myEod Eod
		err = rows.Scan(&myEod.EventDate, &myEod.Ticker, &myEod.Close, &myEod.Dividend, &myEod.SplitFactor)
		if err != nil {
			log.Error().Err(err).Msg("could not scan result in to ticker")
		}

		if currTicker != myEod.Ticker {
			// new ticker, reset
			currTicker = myEod.Ticker
			adjustFactor = 1.0
		}

		myEod.AdjClose = myEod.Close / adjustFactor
		// CRSP adjustment calculations
		// see: http://crsp.org/products/documentation/crsp-calculations
		if myEod.Close > 0 {
			adjustFactor *= (1 + (myEod.Dividend / myEod.Close)) * myEod.SplitFactor
		} else {
			adjustFactor = 1
		}

		fmt.Printf("%s\t%s\t%.2f\t%.2f\n", myEod.EventDate.Format("2006-01-02"), myEod.Ticker, myEod.Close, myEod.AdjClose)
		if _, err := tx.Exec(ctx, "UPDATE eod SET adj_close=$1 WHERE ticker=$2 AND event_date=$3", myEod.AdjClose, myEod.Ticker, myEod.EventDate); err != nil {
			log.Error().Err(err).Str("Ticker", myEod.Ticker).Float64("AdjustedClose", myEod.AdjClose).Float64("Close", myEod.Close).Time("EventDate", myEod.EventDate).Msg("failed to update eod")
			tx.Rollback(ctx)
		}
	}

	tx.Commit(ctx)
	return nil
}
