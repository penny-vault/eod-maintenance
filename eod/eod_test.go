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
package eod_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pashagolub/pgxmock"
	"github.com/penny-vault/eod-maintenance/eod"
)

var _ = Describe("adjust close prices", func() {
	Context("with a dividend", func() {
		It("should adjust the close price", func() {
			ctx := context.Background()
			mock, err := pgxmock.NewConn()
			Expect(err).To(BeNil())
			defer mock.Close(ctx)

			nyc, err := time.LoadLocation("America/New_York")
			Expect(err).To(BeNil())

			rows := mock.NewRows([]string{"event_date", "ticker", "composite_figi", "close", "dividend", "split_factor"}).
				AddRow(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, .25, 1.0).
				AddRow(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 4, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0)

			mock.ExpectQuery("^SELECT (.+) FROM eod WHERE composite_figi = (.+) ORDER BY ticker, event_date DESC$").WillReturnRows(rows)

			prices, err := eod.AdjustAssetEodPrice(ctx, mock, "TEST")
			Expect(err).To(BeNil())

			Expect(prices).To(HaveLen(4))
			Expect(prices[0].EventDate).To(Equal(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc)))
			Expect(prices[0].AdjClose).To(Equal(1.0))

			Expect(prices[1].EventDate).To(Equal(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc)))
			Expect(prices[1].AdjClose).To(Equal(1.0))

			Expect(prices[2].EventDate).To(Equal(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc)))
			Expect(prices[2].AdjClose).To(Equal(.8))
		})
	})

	Context("with a split", func() {
		It("should adjust the close price", func() {
			ctx := context.Background()
			mock, err := pgxmock.NewConn()
			Expect(err).To(BeNil())
			defer mock.Close(ctx)

			nyc, err := time.LoadLocation("America/New_York")
			Expect(err).To(BeNil())

			rows := mock.NewRows([]string{"event_date", "ticker", "composite_figi", "close", "dividend", "split_factor"}).
				AddRow(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 2.0).
				AddRow(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 4, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0)

			mock.ExpectQuery("^SELECT (.+) FROM eod WHERE composite_figi = (.+) ORDER BY ticker, event_date DESC$").WillReturnRows(rows)

			prices, err := eod.AdjustAssetEodPrice(ctx, mock, "TEST")
			Expect(err).To(BeNil())

			Expect(prices).To(HaveLen(4))
			Expect(prices[0].EventDate).To(Equal(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc)))
			Expect(prices[0].AdjClose).To(Equal(1.0))

			Expect(prices[1].EventDate).To(Equal(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc)))
			Expect(prices[1].AdjClose).To(Equal(1.0))

			Expect(prices[2].EventDate).To(Equal(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc)))
			Expect(prices[2].AdjClose).To(Equal(.5))
		})
	})

	Context("with a split and dividend", func() {
		It("should adjust the close price", func() {
			ctx := context.Background()
			mock, err := pgxmock.NewConn()
			Expect(err).To(BeNil())
			defer mock.Close(ctx)

			nyc, err := time.LoadLocation("America/New_York")
			Expect(err).To(BeNil())

			rows := mock.NewRows([]string{"event_date", "ticker", "composite_figi", "close", "dividend", "split_factor"}).
				AddRow(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 1.0, 2.0).
				AddRow(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 4, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0)

			mock.ExpectQuery("^SELECT (.+) FROM eod WHERE composite_figi = (.+) ORDER BY ticker, event_date DESC$").WillReturnRows(rows)

			prices, err := eod.AdjustAssetEodPrice(ctx, mock, "TEST")
			Expect(err).To(BeNil())

			Expect(prices).To(HaveLen(4))
			Expect(prices[0].EventDate).To(Equal(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc)))
			Expect(prices[0].AdjClose).To(Equal(1.0))

			Expect(prices[1].EventDate).To(Equal(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc)))
			Expect(prices[1].AdjClose).To(Equal(1.0))

			Expect(prices[2].EventDate).To(Equal(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc)))
			Expect(prices[2].AdjClose).To(Equal(.25))
		})
	})

	Context("with no splits or dividends", func() {
		It("should adjust the close price", func() {
			ctx := context.Background()
			mock, err := pgxmock.NewConn()
			Expect(err).To(BeNil())
			defer mock.Close(ctx)

			nyc, err := time.LoadLocation("America/New_York")
			Expect(err).To(BeNil())

			rows := mock.NewRows([]string{"event_date", "ticker", "composite_figi", "close", "dividend", "split_factor"}).
				AddRow(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0).
				AddRow(time.Date(2021, 1, 4, 16, 0, 0, 0, nyc), "TEST", "TEST", 1.0, 0.0, 1.0)

			mock.ExpectQuery("^SELECT (.+) FROM eod WHERE composite_figi = (.+) ORDER BY ticker, event_date DESC$").WillReturnRows(rows)

			prices, err := eod.AdjustAssetEodPrice(ctx, mock, "TEST")
			Expect(err).To(BeNil())

			Expect(prices).To(HaveLen(4))
			Expect(prices[0].EventDate).To(Equal(time.Date(2021, 1, 1, 16, 0, 0, 0, nyc)))
			Expect(prices[0].AdjClose).To(Equal(1.0))

			Expect(prices[1].EventDate).To(Equal(time.Date(2021, 1, 2, 16, 0, 0, 0, nyc)))
			Expect(prices[1].AdjClose).To(Equal(1.0))

			Expect(prices[2].EventDate).To(Equal(time.Date(2021, 1, 3, 16, 0, 0, 0, nyc)))
			Expect(prices[2].AdjClose).To(Equal(1.0))

			Expect(prices[3].EventDate).To(Equal(time.Date(2021, 1, 4, 16, 0, 0, 0, nyc)))
			Expect(prices[3].AdjClose).To(Equal(1.0))
		})
	})
})
