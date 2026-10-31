# Plan Step References

The reader of a file must have a link to the plan that was in effect
when someone wrote the file.  Then the reader can compare the old
purpose against the current purpose, and edit the file correctly.

## Definitions

  - Issue Lines: the first two lines of a Back Reference:

      - the issue number and title, in the todos.md format "NNN / Title"; and
      - the plan filename, without the .agentic-shepherd/plans/ prefix.

  - Step Reference: a pointer to one part of a plan, in the format "<section> :: <step>".
  - Back Reference: a comment that points to the plan parts that the code implements.  It holds
    the Issue Lines, then one line for each Step Reference that applies.

## Make a Step Reference

- For example: "Section 15, Commit Boundaries :: Step 6, Staging Files".
- For a section with no steps, do not write the ":: <step>" part.

## Write a Back Reference

Source files, tests, shell scripts, and documents have Back References.  Test fixtures have none.

Use the usual comment prefix of the file.  In Rust, use `//` above any `//!` or `///` lines.
In a document, keep the Back Reference out of the reading flow.  Give the reader a link to
read.  In markdown, put the issue number, the plan filename, and the Step Reference in a
footnote.
Then the body carries the footnote marker alone.  When the file format has no footnotes, use a
link.  When it has no footnotes and no links, write the lines as plain text.

<example_success>
  // 638 / Convert finish-issue skill to a resumable Rust subcommand
  // issue-638-finish-issue-rust.md
  // Section 15, Commit Boundaries :: Step 6, Staging Files
</example_success>

<example_success>
  # Resumable Finish-Issue Workflow[^plan]

  [^plan]:
      638 / Convert finish-issue skill to a resumable Rust subcommand\
      issue-638-finish-issue-rust.md\
      Section 15, Commit Boundaries :: Step 6, Staging Files
</example_success>

## Put the Back References

When you write the file for the plan, put the Back Reference at the top.  When a maximum of
three steps apply, write every step there.

<example_success>
  // 638 / Convert finish-issue skill to a resumable Rust subcommand
  // issue-638-finish-issue-rust.md
  // Section 15, Commit Boundaries :: Step 5, Checklist Adapter
  // Section 15, Commit Boundaries :: Step 6, Staging Files
</example_success>

When more than three steps apply, write no step at the top.  Then put one bare Step Reference on
each struct and function that implements a step.

<example_success>
  // Section 15, Commit Boundaries :: Step 6, Staging Files
  pub fn run_staging_item() {}
</example_success>

If you make the file for a different purpose than the issue, write no Back Reference at
the top.  Then write "See #NNN" in doc comments of structs, fns, or tests that you change.

In a document, put the footnote marker at the title.  When more than three steps apply, put a
marker at each heading that implements a step.
