# Task List Limits

## Scope

- Apply these rules when creating or revising `.kiro/specs/<feature>/tasks.md`.

## Limits

- A spec's task list must contain fewer than 50 task entries: at most 49 total,
  counting both major tasks and sub-tasks.
- Use at most two numbering levels. Major tasks use whole numbers such as `1`;
  sub-tasks use one decimal level such as `1.2`. Do not create sub-sub-tasks
  such as `1.2.1`.

## Complexity reduction

- Do not satisfy the task-count limit by combining work into larger, broader,
  or less executable tasks.
- If a task list would exceed the limit while retaining properly sized tasks,
  reduce implementation complexity in the requirements and design before
  regenerating the task list.
- Do not write or approve `tasks.md` until the plan satisfies both the count
  limit and the normal task-sizing rules.
