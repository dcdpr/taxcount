# Current Context (creating notes suitable for a draft commit)

### [rgrant 20261127 18:59 UTC] 16 / Wallet internal account names are in random order on each run
  - As a user, when I run taxcount two times with the same inputs, I expect the same internal
    account in each report.
  - A wallet transaction can contain TXOs of two or more accounts. For this wallet transaction, the
    internal account contains each account name, with ", " between the names. The order of the
    names is different from run to run.
  - The "Note" column contains values with the same problem.
  - With a random order, a person cannot compare the reports of two runs with a diff. If a test
    compares a wallet report with a fixture, the test also fails.

  - Progress Notes
    - [rgrant 20261127 21:15 UTC] Added concatenate_distinct() in src/basis/split.rs. It puts the
      names into a BTreeSet for the internal account and for the values from notes().
    - [rgrant 20261127 21:15 UTC] [offtopic] Moved the test module of src/basis/split.rs to
      src/basis/split/tests.rs.
    - [rgrant 20261127 21:15 UTC] Added concatenate_distinct_gives_byte_order() in
      src/basis/split/tests.rs.
