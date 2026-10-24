// SPDX-License-Identifier: Apache-2.0

package buildinfo

import "runtime"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// Info describes a build.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
}

// Get returns the build information of the running binary.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: Date,
		GoVersion: runtime.Version(),
	}
}

func (i Info) String() string {
	return "wake " + i.Version + " (commit " + i.Commit + ", built " + i.BuildDate + ", " + i.GoVersion + ")"
}
