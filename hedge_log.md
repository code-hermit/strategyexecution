# Hedge log

Daily record of hedge decisions for the NIFTY + SENSEX straddles, plus margin used and VIX
through the day. Purpose: over time, check whether hedge cost (buy minus sell-back) stays small
against the straddle profits, and tune `hedge_buyer.py`'s buffer settings from real numbers.

Margin figures are AliceBlue's "margin used" (SPAN + exposure, no credit for premium received).
Estimates come from Zerodha's basket-margin API, SPAN + exposure, plus about Rs 8.3k per lot pair
of AliceBlue extra.

---

## 2026-10-27 (Tue) - NIFTY expiry day

**Setup:** 7 lots each of NIFTY and SENSEX straddles (NRML), sensex_option_buying 7 lots. Funds Rs 23.55L.

**Decisions**
- `hedge_buyer.py --plan` at 09:22 picked NIFTY 23100CE/22300PE (±400) and SENSEX 75900CE/70700PE (±2600).
  That's Rs 12,026 for Rs 4.04L spare against its VIX-17.4 target of Rs 4.02L.
- Considered the user's cheaper NIFTY pair 23400CE/22050PE (Rs 1,251). With the same SENSEX hedge it
  gives Rs 1.41L less margin benefit than the program's NIFTY pick, for Rs 3,367 less cost, leaving
  Rs 2.50L spare against Rs 3.90L.
- The user chose to let the program buy (expiry day, unsure). Then **the server's hedge_buyer run
  failed** at 09:37: with an empty order book, AliceBlue's `/orders/book` returned
  `EC916 "No orders found for this user"`, but `_aliceblue_result` only treats `EC920` as empty. So the hedges were placed manually at about 09:40.
- Final legs, chosen live at 09:38 as the best cost/margin value (NIFTY 22,730, SENSEX 73,456, VIX 17.08):

  | Leg | Qty | Ask at 09:38 | Cost |
  |---|---|---|---|
  | NIFTY26OCT23250CE | 455 | 1.80 | Rs 819 |
  | NIFTY26OCT22250PE | 455 | 2.45 | Rs 1,115 |
  | SENSEX26OCT76500CE | 140 | 9.30 | Rs 1,302 |
  | SENSEX26OCT70500PE | 140 | 16.20 | Rs 2,268 |
  | **Total** | | | **about Rs 5,504** (about half the program's Rs 11.3k pick) |

- Estimate: NIFTY Rs 10.65L + SENSEX Rs 8.56L + extra Rs 0.58L = **Rs 19.8L used**, about **Rs 3.0L spare**
  (including Rs 0.75L for option buying). Per combined lot: Rs 2.83L selling margin, against the
  planning budget of Rs 3.09L.
- To do: sell these 4 legs manually at about 15:15. The server close run has no state today.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|
| 09:45 | Rs 19.68L | 17.09 | 22,752 | 73,533 | Straddles entered. Matches the Rs 19.8L estimate. |

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.
