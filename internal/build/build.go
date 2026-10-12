// Package build carries the identity stamped into the binaries at link time.
//
// The values are set by the Makefile via -ldflags -X from `git describe`, so a
// released binary names the semver tag it was cut from, a snapshot build names
// the commit, and a `go run` from a working tree says "dev" — three answers to
// "what exactly is running in production", none of them guesses.
package build

var (
	// Version is the semantic version (or `git describe` output) of this build.
	Version = "dev"
	// Hash is the short git commit hash the build was produced from.
	Hash = ""
)

// String renders the identity the way the -version flag and startup logs show it.
func String() string {
	if Hash == "" {
		return Version
	}
	return Version + " (" + Hash + ")"
}
