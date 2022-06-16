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
	"github.com/penny-vault/eod-maintenance/eod"
	"github.com/spf13/cobra"
)

var all bool

// adjustedCmd represents the adjusted command
var adjustCmd = &cobra.Command{
	Use:   "adjust",
	Short: "Calculate adjusted eod prices",
	Run: func(cmd *cobra.Command, args []string) {
		eod.AdjustTickers(all)
	},
}

func init() {
	rootCmd.AddCommand(adjustCmd)

	adjustCmd.Flags().BoolVarP(&all, "all", "a", false, "calculated adjusted price for all eod tickers")
}
