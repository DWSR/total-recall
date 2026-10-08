# GitHub-Native Stack Implementation

## Scope and precedence

- Apply these rules when implementing tasks from `.kiro/specs/`.
- Use GitHub-native Stacks for branch and pull request orchestration. Do not
  use Graphite for spec implementation or treat a chain of dependent pull
  request bases as a substitute for a native Stack object.
- These rules supersede Graphite-specific guidance in repository steering,
  task plans, and agent instructions for spec implementation only.
- Keep each Stack linear and order its members bottom-to-top according to the
  tasks' `_Depends:_` relationships. When dependencies do not determine an
  order, follow the approved task order.

## Commit boundaries

- Default to one executable Kiro task per Stack member pull request and one
  bisectable commit per member.
- Each commit must represent one coherent behavior and leave the repository in
  a working state. Relevant tests, builds, and static checks must pass at that
  commit, not only at the top of the Stack.
- Put a change and its focused tests in the same commit. Unit tests, fixtures,
  and test helpers that directly validate a change belong beside that change;
  do not split implementation and its focused tests into separate commits.
- Keep unrelated refactors, cleanup, and formatting out of the commit. If they
  are required prerequisites, give them their own earlier bisectable Stack
  members.

## Larger test scopes

- A dedicated test commit is allowed when integration, end-to-end, system, or
  other cross-cutting tests require behavior from multiple earlier commits.
- Place that test commit directly after its last dependency: the highest Stack
  member whose behavior the tests require. Do not place unrelated work between
  the dependency and its test commit.
- The dedicated test commit must contain only the cross-cutting tests and the
  fixtures or harness changes they require, and must itself leave the
  repository passing.
- Treat the test commit as a dependency of every later Stack member that relies
  on the behavior it validates.

## Verification

- Verify every member commit independently against its parent before creating
  or updating its pull request.
- After changing a lower Stack member, update the members above it and repeat
  per-commit verification.
- Do not mark a task complete until its commit is verified and its pull request
  is linked in the GitHub-native Stack in the intended dependency order.
