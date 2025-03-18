## [rgrant 20231027 20:54 UTC] notes on settling versus closing margin positions

```
  https://support.kraken.com/hc/en-us/articles/202966956-Settling-or-closing-a-spot-position-on-margin

    / How settling a position works /

    You may close all or part of a spot position on margin by
    transferring to us, directly from your account balance with no
    trade involved, funds of the type used by Kraken to make the
    initial margin extension (e.g., if you took an extension of margin
    from Kraken denominated in BTC, you must have sufficient BTC in
    your account to settle the position).  This is called position
    settlement.  If your account balance is not sufficient, you may
    deposit additional funds into your account.  If you have
    sufficient funds in your account, but they are not of the type
    used by Kraken to make the initial margin extension, you may
    execute an order for the type and amount of funds you need to
    settle the position (e.g., buy 1 BTC of BTC/USD).Note: If you have
    multiple open positions on margin, they will be closed in the
    order they were created following the "First In, First Out" (FIFO)
    rule.

    / How closing transactions work /

    Through a closing transaction, you may partially or fully close a
    spot position on margin by executing an opposing order for up to
    the same volume as the order that opened your position (a sell
    closes a “long” spot position on margin and a buy closes a “short”
    spot position on margin).  The proceeds from a closing transaction
    are applied first to the satisfaction of your margin obligation to
    Kraken.  Then, any remaining profit (or loss) is added to (or
    taken from) your account balances, in an amount denominated in the
    quote currency of the pair you are trading (e.g., EUR in the pair
    BTC/EUR).

    If you have multiple collateral currencies, when your loss is
    realized (regardless of if you close it yourself or it's closed
    via an automated liquidation), it will be deducted in the
    following order of preference: [...]
```

### [rgrant 20231027 20:57 GMT] tldr:
  - settling is giving them back what they loaned you, straight from your balances
  - closing is generating an opposite trade to cancel the obligation
