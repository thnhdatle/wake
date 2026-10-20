// SPDX-License-Identifier: Apache-2.0

// Package fakesink is an in-memory pipeline.Sink for tests: it records every
// record it receives, holds them until the test acknowledges them, blocks
// Send while its bounded buffer of unacknowledged records is full, and lets
// the test set its health and inject Send errors.
//
//	s := fakesink.New("kafka-main", fakesink.WithCapacity(2))
//	_ = s.Send(ctx, rec)
//	_, err := s.AckNext(1) // acknowledge the oldest pending record
package fakesink
