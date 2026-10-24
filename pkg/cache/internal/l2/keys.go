// SPDX-License-Identifier: Apache-2.0

package l2

import "strconv"

func KeyNames(namespace string, epoch uint64, key string) (value, gen, lock string) {
	prefix := namespace + ":e" + strconv.FormatUint(epoch, 10) + ":{" + key + "}:"
	return prefix + "v", prefix + "g", prefix + "l"
}

func EpochKey(namespace string) string {
	return namespace + ":epoch"
}

func DefaultChannel(namespace string) string {
	return "wake:inval:" + namespace
}
