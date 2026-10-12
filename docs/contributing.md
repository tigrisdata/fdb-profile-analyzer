# Contributing

How changes get into fdb-profile-analyzer: the commit convention that drives releases, what
a pull request has to pass, and how to run the same checks before pushing.

## Commit messages name their version bump

Every commit subject starts with one of three prefixes:

| Prefix | Meaning | Release effect |
| --- | --- | --- |
| `major: ` | an incompatible change — a ClickHouse table shape, a config key, a dashboard query an operator relies on | the major version increases |
| `minor: ` | a new capability that changes nothing existing | the minor version increases |
| `patch: ` | a fix, a refinement, a doc or test change | the patch version increases |

```
patch: hex columns say (empty) for the empty key
minor: label unmatched keys by their first 16 bytes
major: the ingest cursor keys on the cluster name
```

The rule is enforced twice:

- **Locally**, by a `commit-msg` hook that refuses a commit whose subject has no prefix.
  Hooks do not ship enabled with a clone, so run once per clone:

  ```console
  $ make hooks        # git config core.hooksPath .githooks
  ```

  Merge commits pass, and so do `fixup!` / `squash!` commits, which `git rebase -i
  --autosquash` folds into a prefixed commit before the pull request is merged.

- **In CI**, by the `commit-messages` job on every pull request, which checks every non-merge
  commit between the base and the head. A `fixup!` that survived to the pull request fails
  there.

Write the prefix for what the change *does*, not for its size: a one-line change to a
table's sorting key is `major`, a thousand-line new dashboard panel is `minor`.

## Every merge to main is a release

The `release` workflow runs on every push to `main`. It reads the subjects (and bodies, so a
squash merge's list of commits counts) of the non-merge commits since the last `v*` tag,
takes the **highest** prefix among them, bumps the last version accordingly, pushes the tag
`vX.Y.Z`, and publishes a GitHub release with notes generated from the merged pull requests.
The release carries:

| Asset | What it is |
| --- | --- |
| `fdbprof-linux-amd64` | the ingest daemon; `fdbprof -version` prints the tag. It links against `libfdb_c`, so the host needs FoundationDB's client package, as every FoundationDB client does |
| `checksums.txt` | its sha256 |

So a pull request decides the version it ships as through its own commits: a `major` commit
anywhere in it makes the next release a major one. `make dist VERSION=vX.Y.Z` builds the same
files locally, in the Go container. A second job of the release builds the repo-root
`Dockerfile` and pushes it to `quay.io/tigrisdata/fdb-profile-analyzer`, tagged `vX.Y.Z`,
`latest` and `github-<short sha>`, with the `QUAY_USER` and `QUAY_TOKEN` repository secrets
(README, "CI and releases", says how to set them up). `make image` builds the same image
locally, stamped from `git describe`.

## What a pull request has to pass

Five jobs, separate so a red one reads at a glance (`.github/workflows/go-test.yaml`), plus
`yamllint`:

| Job | What runs | Locally |
| --- | --- | --- |
| commit messages | every commit subject carries a prefix | the hook, `make hooks` |
| unit tests | build, vet (the integration suite too) and the unit tests | `make test` |
| integration suite | a fresh docker-compose test bed, every test, torn down afterwards; `test-results/` is kept as an artifact when it fails | `make integration_test` |
| integration suite, sharded | the same suite against a two-shard ClickHouse | `make integration_test_sharded` |
| image | the repo-root `Dockerfile` builds for linux/amd64, without pushing | `make image` |

The Go bindings link against `libfdb_c`, so the machine that runs the tests — the CI runner
or yours — needs FoundationDB's client package even though everything under test runs in
Docker (`make deps_check`; README, "Running Go tests against it"). Branch protection on
`main` should require all five.

## Running the suites while you work

```console
$ make testenv_up && make testenv_verify    # fresh test bed, every service checked
$ make testenv_smoke                        # load -> FoundationDB -> daemon -> ClickHouse
$ make integration_test                     # the verdict: fresh bed, every test, torn down
$ make integration_test_sharded             # the same against two ClickHouse shards
$ make testenv_down
```

A reused test bed is fine while iterating on a fix; the verdict is always a full
`make integration_test` (and the sharded run) on a fresh bed, which is what the pull
request's checks do.

## Before you start on a subsystem

Read [SPEC.md](SPEC.md) for the pipeline — the profile key and record formats, the tailer's
contract, the dashboard — and [SCHEMA.md](SCHEMA.md) for the ClickHouse tables, column by
column. The DDL under `schema/` is the product's data contract, not test-bed detail: name
the bump a change to it deserves.
