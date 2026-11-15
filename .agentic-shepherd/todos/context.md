# Current Context (creating notes suitable for a draft commit)

### [rgrant 20261113 21:59 UTC] 09 / Proceeds are 0.0000 for each margin close in event details file
  - As a user, when I read a "Position" line in the event details file, I expect a USD value in its
    "Proceeds" column. That value is the profit or the loss of the margin close.
  - Each line with "Position" in the "Event" column is a margin close, and its "Proceeds" column
    contains "0.0000".
  - For a margin close in USD, the user can read the USD value from the "Asset Amount" column. For
    a margin close in EUR, CHF, or JPY, the USD value is missing from the event details file.
  - In references/kraken-tests, line "fe7af8-f0c7b-45b4a5" contains 11.8707 EUR in the "Asset
    Amount" column and 0.0000 in the "Proceeds" column. The ledgers CSV contains 12.7859 USD for
    the same margin close.
  - The "Fees" column of that line contains the fee share 1.4370. "Proceeds" minus "Fees" is
    -1.4370 on that line, and the correct value is 11.3489.
  - The sums CSV and the ledgers CSV contain the correct values for each margin close.

  - [rgrant 20261115 18:14 UTC] Analysis
    - The fmt() function of CapGainsEventDetails writes proceeds().unwrap_or_default() of each
      EventAtom in the "Proceeds" column. EventAtom::proceeds() returns None for
      EventAtom::Position. As a result, the "Proceeds" column contains 0.0000 for each position
      atom.
    - A position atom contains its proceeds in two values, proceeds_us and proceeds_bona_fide.
      event_proceeds() and the sums() function of CapGainsWorksheet read these two values directly.
      The ledgers CSV and the sums CSV contain the correct values.
    - Event::add_fee() is the only other caller of EventAtom::proceeds(). Event::add_fee() calls
      EventAtom::proceeds() on fee atoms only.
    - The commit "[WIP]: fee-rework implementation attempt 1" of 20260921 added
      EventAtom::proceeds() with None for EventAtom::Position.
    - fixtures/runs/kraken-tests/20261031-ledgers-event-details.csv contains 0.0000 for line
      "fe7af8-f0c7b-45b4a5". The commit "20261106 an implementation of issue #04" added the "Fees"
      column after that file.

  - Progress Notes
    - [rgrant 20261115 18:22 UTC] EventAtom::proceeds() returns a UsdAmount. For a position atom,
      the value is the sum of proceeds_us and proceeds_bona_fide.
    - [rgrant 20261115 18:22 UTC] event_proceeds() calls EventAtom::proceeds() for trade atoms,
      income atoms, and position atoms.
    - [rgrant 20261115 18:22 UTC] src/model/gains/tests.rs contains the new tests
      margin_close_proceeds_in_event_details and position_proceeds_us_and_bona_fide.
    - [rgrant 20261115 18:22 UTC] In a run of references/kraken-tests, line "fe7af8-f0c7b-45b4a5"
      of the event details file contains 12.7859 in the "Proceeds" column and 1.4370 in the "Fees"
      column.
