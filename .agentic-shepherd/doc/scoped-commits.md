# Commit Structure, Including Multi-Issue Commits

## Background

We use a structured format for commit messages:

```
scope (#NNN): <description>
```

Scope is generally guided by the crates and modules touched.  It is
the most important part of the commit message: what area of concern
should the programmer reading the commit message be made aware of?

Scopes other than Rust crates and modules include jp/config and
tests/.  There are others.

If there is more than one scope, separate by commas, like
```
scope1, scope2: <description>
```

The issue number is next.  If there are several then separate by
commas.  Include the "#" each time.

## Write From User's Perspective, Concisely

Be laconic.  A commit message is a changelog entry, not an essay.
Keep each line short and purposeful.  No throat-clearing, no recap of
the diff, no padding.

Do not restate the code or the spec.  The reader has access to
both.  Reference behaviors, capabilities, and fixes by name; do not
paraphrase the source back at the reader.

Speak from the user's perspective.  Describe what changed for the
user of the system — the behavior, the capability, the fix.
Implementation detail belongs in the code, not in the message.

Use only list items preceded by '- ', no prose.  Cover all broad
purposes of changes: what behavior changed, what capability was added,
what problem was fixed, what structural improvement was made.

Do not use `backticks` for code expressions unless the expression
spans a space, and even then not if it is bound in (parenthesis).

Do not use **highlights**.  At all.

Useful: changes visible to the user or changes in system behavior.

Not useful: naming internal functions, structs, or modules that were
added or modified in service of a purpose.

Caveat: for refactoring and infrastructure work, the structural
description IS the purpose - type and function names belong because
there is no user-facing behavior to describe instead.

## Multi-Issue Commits in Practice (how things work here)

Projects that use wip-commit workflows regularly produce multi-issue
commits where changes accumulate on a single branch.  A single commit
may contain features, fixes, and unfinished work across several
issues.

The long-term fix is per-issue branches via git-worktree, which
produces single-issue commits naturally.  Until then, multi-issue
commits are an accepted reality.

## Title Format for Multi-Issue Commits

When a commit contains work from multiple issues:

- Single scope: 'scope(#N1, #N2): summary' or
  'scope(#N1): summary'
- Multiple scopes: list each scope in its own section, separated by
  periods: 'scope1(#N1, #N2): summary. scope2(#N3): summary. wip(#N4, #N5).'
- No issue: 'scope: summary'

All issue numbers (other than unrelated issues that have merely been
newly filed) must appear in the title.  The per-issue sections in the
commit body provide the detail.

Keep the title line under 100 characters.  If you run out of room,
drop summaries from the title.

## Dealing With Newly Filed Issues

Newly filed issues are documented by virtue of their existence in the
database, and should not receive any mention in commits UNLESS the
newly filed issues are either: work related to an issue directly
addressed by the commit, such as splitting off sub-issues; or the sole
contents of a commit that is only adding new issues.
