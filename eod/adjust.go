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
)

func AdjustAssetEodPrice(ctx context.Context, conn *pgx.Conn, compositeFigi string) ([]*Eod, error) {
	adjustHistory := make([]*Eod, 0)
	adjustFactor := 1.0

	rows, err := conn.Query(ctx, "SELECT event_date, ticker, composite_figi, close, dividend, split_factor FROM eod WHERE composite_figi = $1 ORDER BY ticker, event_date DESC", compositeFigi)
	if err != nil {
		log.Error().Err(err).Msg("SELECT all query error")
		return adjustHistory, err
	}

	for rows.Next() {
		var myEod Eod
		err = rows.Scan(&myEod.EventDate, &myEod.Ticker, &myEod.CompositeFigi, &myEod.Close, &myEod.Dividend, &myEod.SplitFactor)
		if err != nil {
			log.Error().Err(err).Msg("could not scan result into eod")
			return adjustHistory, err
		}

		myEod.AdjClose = myEod.Close / adjustFactor
		// CRSP adjustment calculations
		// see: http://crsp.org/products/documentation/crsp-calculations
		if myEod.Close > 0 {
			adjustFactor *= (1 + (myEod.Dividend / myEod.Close)) * myEod.SplitFactor
		} else {
			adjustFactor = 1
		}

		adjustHistory = append(adjustHistory, &myEod)
	}

	return adjustHistory, nil
}

// SaveAdjCloseToDb updates database record with adjusted close value
func SaveAdjCloseToDb(ctx context.Context, conn *pgx.Conn, prices []*Eod) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not begin db transaction to adjust eod prices")
	}

	for _, myEod := range prices {
		if _, err := tx.Exec(ctx, "UPDATE eod SET adj_close=$1 WHERE composite_figi=$2 AND event_date=$3", myEod.AdjClose, myEod.CompositeFigi, myEod.EventDate); err != nil {
			log.Error().Err(err).Str("Ticker", myEod.Ticker).Float64("AdjustedClose", myEod.AdjClose).Float64("Close", myEod.Close).Time("EventDate", myEod.EventDate).Msg("failed to update eod")
			if err2 := tx.Rollback(ctx); err2 != nil {
				log.Error().Err(err).Msg("failed to rollback db transaction")
			}
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("could not commit eod price update to database")
		return err
	}

	return nil
}
