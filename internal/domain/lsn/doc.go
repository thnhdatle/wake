// SPDX-License-Identifier: Apache-2.0

// Package lsn provides the Postgres log sequence number type: a 64-bit WAL
// position written as two upper-case hexadecimal halves separated by a slash.
//
//	l, err := lsn.Parse("0/16B3748")
//	if err != nil { ... }
//	next := l + 8
//	fmt.Println(next) // 0/16B3750
package lsn
