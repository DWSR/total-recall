# Spec Pull Request Size Limits

## Scope

- Apply these rules to pull requests that implement work from `.kiro/specs/`.
- Measure pull request size as total added and deleted lines relative to the
  intended base branch.

## Limits

- Aim for 500 changed lines or fewer per pull request.
- Keep each pull request at 1,000 changed lines or fewer unless a human
  explicitly approves an exception.
- Treat these limits as implementation constraints during design, task
  decomposition, implementation, and review. Prefer simplifying the
  implementation over allowing a pull request to grow beyond them.

## Exception gate

- As soon as a pull request is expected to exceed 1,000 changed lines, stop
  work and ask the human to choose exactly one of these outcomes:
  1. Accept the larger pull request and provide a justification for the
     exception.
  2. Direct the agent to rework and simplify the implementation until the pull
     request is 1,000 changed lines or fewer.
  3. Stop the work.
- Do not continue implementation or create, update, or publish the oversized
  pull request until the human explicitly chooses an outcome.
- Record the human's justification in the pull request description when an
  exception is approved.
