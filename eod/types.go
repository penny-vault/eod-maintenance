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

import "time"

type Eod struct {
	EventDate     time.Time
	EventDateStr  string `csv:"date"`
	Ticker        string
	CompositeFigi string
	Close         float64
	AdjClose      float64 `csv:"adjClose"`
	Dividend      float64
	SplitFactor   float64
}

type SyntheticAsset struct {
	Category      string
	Components    []*SyntheticComponent
	CompositeFigi string
	Name          string
	StartDate     time.Time
	Symbol        string
}

type SyntheticComponent struct {
	CompositeFigi string
	FileName      string
	Name          string
	Start         time.Time
	Source        []string
	Symbol        string
	End           time.Time
}

type PercentChange struct {
	Date    time.Time
	Percent float64
}
