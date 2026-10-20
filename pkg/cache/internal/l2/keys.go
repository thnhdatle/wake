// SPDX-License-Identifier: Apache-2.0

package l2

import "strconv"

// KeyNames returns the value, generation and lock key names of key in the
// given namespace and epoch: <ns>:e<epoch>:{<key>}:v, :g and :l.
func KeyNames(namespace string, epoch uint64, key string) (value, gen, lock string) {
	prefix := namespace + ":e" + strconv.FormatUint(epoch, 10) + ":{" + key + "}:"
	return prefix + "v", prefix + "g", prefix + "l"
}

// EpochKey returns the name of the key holding the namespace epoch.
func EpochKey(namespace string) string {
	return namespace + ":epoch"
}

// DefaultChannel returns the default invalidation bus channel of a namespace.
func DefaultChannel(namespace string) string {
	return "wake:inval:" + namespace
}
