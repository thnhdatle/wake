// SPDX-License-Identifier: Apache-2.0

package buildinfo

import "runtime"

// Link-time variables set with -ldflags "-X .../buildinfo.Version=...".
// They are the only package-level variables of this package and are never
// modified at run time.
var (
	// Version is the release version, "dev" for local builds.
	Version = "dev"
	// Commit is the git commit the binary was built from.
	Commit = "none"
	// Date is the build date in RFC 3339 format.
	Date = "unknown"
)

// Info describes a build. JSON tags match GET /v1/version.
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

// String renders the information on one line for "wake version".
func (i Info) String() string {
	return "wake " + i.Version + " (commit " + i.Commit + ", built " + i.BuildDate + ", " + i.GoVersion + ")"
}
