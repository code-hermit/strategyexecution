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

---

## 2026-10-28 (Wed) - SENSEX expires tomorrow, NIFTY on the new weekly (03 Nov)

**Setup:** 7 lots each, NRML. Funds Rs 23.47L. The EC916 empty-order-book fix is in `hedge_buyer.py`
locally (an empty `/positions` returns EC920, already handled). It still needs to be on the server
before 09:37.

**Decisions**
- `hedge_buyer.py --plan` at 09:30 (NIFTY 22,553, SENSEX 72,778, VIX 17.07):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0323800CE + NIFTY26N0321300PE (±1250) | 455 | 3.80 + 6.00 | Rs 4,459 | Rs 10.26L |
  | SENSEX26OCT75400CE + SENSEX26OCT70200PE (±2600) | 140 | 8.20 + 6.65 | Rs 2,079 | Rs 8.19L |
  | **Total** | | | **Rs 6,538** | |

- Estimate: Rs 19.63L used (including Rs 0.58L extra and Rs 0.61L option buying), **Rs 3.83L spare**
  against a Rs 3.80L target. Accepted as is. NIFTY is far out because 6-day options near the money are
  expensive (±800 = Rs 16k). SENSEX gives the relief cheaply on the day before its expiry.
- Expected net cost after the 15:15 sell-back: roughly Rs 2.5-3.5k. The NIFTY legs keep most of their value.
- **09:3x: broker notice of an extra 2% exposure margin today.** On short notional (both legs) that's about
  Rs 4.1L per index at 7 lots, and hedges can't offset it. Live check at 09:39 with the hedges already
  held: 7 lots needs Rs 28.16L (Rs 4.73L short), 6 lots Rs 23.62L (Rs 0.19L short), 5 lots Rs 19.10L
  (Rs 4.31L spare).
- The server hedge_buyer run **worked** today (EC916 fix deployed): it bought NIFTY26N0323850CE +
  NIFTY26N0321250PE x455 and SENSEX26OCT75400CE + SENSEX26OCT70400PE x140 (7-lot hedges).
- **Decision: straddles cut to 5 lots for today**. `lots=5` was set by hand in exec_rsv_adjust_sl.py on
  the server just before 09:44. sensex_option_buying stays at 7 lots (premium from cash). The 7-lot
  hedges over-cover the 5-lot straddles. Local repo copy still says 7. Revert the server to 7 tomorrow.
- `hedge_buyer.py` now reads `EXTRA_EXPOSURE_PCT` from .env (percent of short notional, both legs) for
  days like this. Set it to 2 when the broker announces one, then back to 0.
- **Correction (after entry):** AliceBlue showed margin used **Rs 12.22L** at 5 lots: SPAN Rs 5.89L +
  exposure Rs 5.86L. The exposure is just the normal 2% of short notional, so the broker's "additional 2%"
  was **not** being charged at entry. My 7-lot "doesn't fit" call was wrong: 7 lots would have used about
  Rs 17L. Staying at 5 lots for the rest of the day (no mid-session restart); back to 7 tomorrow.
  Lesson: treat a broker notice as unconfirmed. Show both cases and check AliceBlue's exposure figure after
  entry before cutting lots. Keep EXTRA_EXPOSURE_PCT=0 unless the charge actually appears.
- **Clarified: the broker's extra 2% applies to SENSEX shorts only** (expires tomorrow), not NIFTY.
  Recalculated at 09:50 (NIFTY 22,650, SENSEX 73,150, VIX 16.4, held hedges, funds Rs 23.43L):
  NIFTY 5 + SENSEX 7 (+Rs 4.10L extra) = Rs 20.9L, Rs 2.6L spare. NIFTY 7 + SENSEX 7 = Rs 24.8L, Rs 1.4L
  short. A closer SENSEX hedge (Rs 9-27k) would only leave Rs 0.4-1.4L spare, so not worth it.
- **Decision:** at about 10:43, SENSEX goes to 7 lots (server: SENSEX line `lots=7`, restart
  option_selling_sensex only). NIFTY stays at 5 today. If SENSEX exposure after going to 7 shows only the
  normal ~Rs 4.1L (no extra), NIFTY 7 would fit (about Rs 20.7L, Rs 2.7L spare). Re-check then.
- Note: AliceBlue `tradingLimit` falls intraday as margin is used (Rs 12.86L at 09:50), so `--plan` run
  mid-session understates funds. Fine for the 09:37 run. To fix in hedge_buyer.
- **Final for today:** the user chose **NIFTY 7 + SENSEX 5** (NIFTY carries no extra charge, so it is the
  cheaper index to add lots to). At 09:55 the user sold 130 more NIFTY 22600 CE and PE by hand (to 455)
  and restarted the NIFTY strategy with lots=7 at about 09:57. Verified via AliceBlue at 09:57: CE/PE
  -455 (avg 206.86 / 196.44), SLs updated in place to BUY 455 (trigger 238 / 226, tags ...0945, no
  leftover 325 SLs), hedges +455. SENSEX CE already stopped out; 73000 PE -100 open with its SL pending.
  NIFTY's daily loss limit restarted at about 09:57.
- **Correction 2 (10:46):** my NIFTY 7 + SENSEX 5 estimate was Rs 20.6L; actual was **Rs 16.03L**. Errors:
  (1) I included the SENSEX extra 2% (Rs 2.93L) again, though it still isn't charged; (2) I counted the
  option-buying premium (Rs 0.61L) as margin, but it's cash and never shows in margin used; (3) about
  Rs 1L less SPAN than estimated (7-lot SENSEX hedges over 5 lots, spot moved). With no extra charge,
  SENSEX 7 at the next hour would be about Rs 17.6L used, Rs 5.8L free.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|
| ~09:50 | Rs 12.22L (SPAN 5.89L + exp 5.86L) | ~16.9 | ~22,580 | ~72,930 | 5 lots. Extra 2% not charged. Rs 11.28L free. |
| 09:57 | Rs 14.35L (SPAN 8.21L + exp 5.58L) | ~16.4 | ~22,600 | ~73,000 | NIFTY 7 lots, SENSEX PE only (5 lots). Extra 2% still not charged. Rs 9.15L free. |
| 10:46 | Rs 16.03L (SPAN 8.29L + exp 7.01L) | - | ~22,550 | ~72,800 | NIFTY 7 + SENSEX 5, both straddles open. Exposure = normal 2% only. Rs 7.40L free. |
| ~10:51 | Rs 18.5L | - | - | ~72,800 | SENSEX raised to 7 (40 CE + 40 PE sold by hand). Rs 5L spare. SENSEX restart pending to resize SLs (CE SL still 100). |
| 10:54 | Rs 16.81L (SPAN 9.32L + exp 6.73L) | - | ~22,550 | ~72,800 | SENSEX restarted. PE -140 with SL 140. The CE 100-qty SL (trigger 414) fired before the restart, leaving -40 CE (the hand-sold lots); the restart put a 40-qty SL at 435 on it. Suggested buying back the 40 CE so the CE side is flat until 11:45, as the strategy would be. Rs 6.66L free. |
| 12:45 | Rs 19.45L | - | - | - | All 7 lots: NIFTY 7 + SENSEX 7, both legs. Close to the morning plan's Rs 19.02L for 7+7 (Rs 19.63L less Rs 0.61L option buying). About Rs 4.0L free on Rs 23.47L funds. |

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-10-29 (Thu) - SENSEX expiry day, NIFTY on the 03 Nov weekly

**Setup:** 7 lots each, NRML. Funds Rs 23.32L. VIX 16.30.

**Decisions**
- `hedge_buyer.py --plan` at 09:22, pre-open prices (NIFTY ATM about 22,700, SENSEX 73,258):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0323700CE + NIFTY26N0321700PE (±1000) | 455 | 3.10 + 6.10 | Rs 4,186 | Rs 8.65L |
  | SENSEX26OCT74800CE + SENSEX26OCT71800PE (±1500) | 140 | 4.45 + 6.45 | Rs 1,526 | Rs 10.44L |
  | **Total** | | | **Rs 5,712** | |

- Estimate Rs 20.02L, including Rs 0.58L AliceBlue extra and Rs 0.35L option buying. Margin used should
  be about Rs 19.7L, since option buying is cash. Spare Rs 3.30L against a Rs 3.23L target. SENSEX
  exposure is about 4% today because it expires. The program re-prices at 09:38.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.
