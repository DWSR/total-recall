# Rust and Nix build hygiene

Use this steering in Rust repositories that also use Nix flakes. Its purpose is
to prevent Cargo build artifacts from being copied into the Nix store or local
binary cache as multi-gigabyte source snapshots.

## Core rule

Nix flake inputs must describe source code, not the mutable build workspace.
Cargo artifacts belong outside every source tree that Nix may snapshot.

## Flake references

Use Git-backed flake references for normal development commands:

```sh
nix develop
nix build .#<package>
nix flake check
```

Do not use an explicit path flake against a working tree:

```sh
# Do not use these against a Rust worktree.
nix develop path:.
nix build path:.#<package>
nix flake check path:.
```

A Git-backed flake includes tracked source and dirty modifications while
honoring Git's source filtering. A `path:` flake snapshots the directory as a
filesystem tree. It can copy ignored `target/`, `.direnv/`, coverage output,
test fixtures, and other generated data into `/nix/store` before `flake.nix`
is evaluated.

If a sandbox cannot read the user's Git configuration, fix that environment
instead of changing the flake to `path:`. On the managed OpenCode hosts, use:

```sh
HOME=/var/empty nix build .#<package>
HOME=/var/empty nix flake check
```

If a build genuinely needs an untracked file, add that file to the repository
or construct an explicitly filtered staging source. Do not expose the entire
working tree to Nix to make one untracked file visible.

## Cargo artifact location

Every Rust repository must ignore Cargo output:

```gitignore
target/
.direnv/
```

For repositories frequently evaluated by Nix or automation, keep interactive
Cargo output outside the repository as a second line of defense:

```sh
export CARGO_TARGET_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/cargo-target/<project>"
```

Set this in the development shell or direnv configuration, not in Nix sandbox
builders. Use a stable, project-specific directory so separate repositories do
not accidentally share incompatible artifacts.

Moving `CARGO_TARGET_DIR` does not bound Cargo's disk usage. Clean or rotate the
external directory separately when it becomes large. Its location prevents Nix
source ingestion; it is not a retention policy.

## Derivation source filtering

Rust package derivations must pass a filtered source to the builder. Prefer
Nix filesets that name the source-bearing paths:

```nix
src = lib.fileset.toSource {
  root = ./.;
  fileset = lib.fileset.unions [
    ./Cargo.toml
    ./Cargo.lock
    ./src
    ./crates
  ];
};
```

Adapt the fileset to the workspace. Include migrations, protocol definitions,
build scripts, fixtures, and other compile-time inputs when required. Do not
include `target/`, editor state, local databases, logs, or generated reports.

`lib.cleanSource`, `lib.cleanSourceWith`, and filesets filter a derivation's
`src`. They do not rescue an explicit `path:` flake: Nix must first copy the
flake tree into the store before it can evaluate those filters. Source filtering
and Git-backed flake references solve different layers of the problem; use both.

## Agent behavior

Before running Nix in a Rust repository:

- Check that the command uses `.` or `.#...`, not `path:.`.
- Check that `target/` and `.direnv/` are ignored.
- Prefer the repository's existing dev shell and build commands.
- Do not add generated artifacts to Git to make a flake evaluation succeed.
- Do not weaken source filtering as a workaround for a missing build input.
- Do not run garbage collection or delete Cargo output without user approval.

When introducing a Nix package for a Rust workspace:

- Use an explicit fileset or an equivalently strict source filter.
- Keep `Cargo.lock` in the source set for reproducible application builds.
- Verify that build scripts can see every declared non-Rust input.
- Exercise the package from a dirty worktree to ensure ordinary source edits
  are visible without admitting ignored artifacts.

## Diagnosis

Suspect source ingestion when `/nix/store` contains repeated large paths named
`*-source`. Inspect one before changing retention policy:

```sh
du -sh /nix/store/<hash>-source/*
```

If `target/` dominates the path, find and remove the `path:` flake reference or
unfiltered source import that created it. More frequent garbage collection only
treats the symptom.

Distinguish the storage layers:

- Cargo's `target/` contains mutable local compiler artifacts.
- `/nix/store` contains immutable inputs and outputs retained by Nix roots.
- A binary cache such as Attic retains published Nix outputs under its own
  retention and garbage-collection policy.

Cleaning one layer does not necessarily clean the others.

## Recovery

After fixing the source boundary, reclaim local Cargo artifacts with:

```sh
cargo clean
```

Reclaim unreachable Nix paths with the host's configured retention command:

```sh
sudo nix-collect-garbage --delete-older-than 7d
```

Both commands are destructive. Agents must present the expected scope and get
explicit approval before running them. Garbage collection is the final recovery
step, not the prevention mechanism.

## Review checklist

A Rust and Nix change is ready when:

- Development and CI commands use Git-backed flakes.
- Cargo output cannot enter the flake source tree.
- Package derivations use filtered source inputs.
- Required build-time files remain present after filtering.
- Verification does not rely on `path:` as a sandbox workaround.
- Cleanup and retention remain separate from build correctness.
