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

---

## 2026-10-30 (Fri) - NIFTY expires 03 Nov, SENSEX 05 Nov

**Setup:** 7 lots each, NRML. Funds Rs 23.31L. VIX 15.56. Spare settings cut this morning (commit eb680df):
SPARE_BASE 3L -> 1.5L, SPARE_PER_VIX_POINT 75k -> 50k, SPARE_MIN 1.5L -> 1L.

**Decisions**
- `hedge_buyer.py --plan` at 09:21, pre-open prices (NIFTY 22,950, SENSEX 74,216):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0324400CE + NIFTY26N0321500PE (±1450) | 455 | 0.75 + 1.70 | Rs 1,115 | Rs 10.63L |
  | SENSEX26N0578000CE + SENSEX26N0570400PE (±3800) | 140 | 8.05 + 23.50 | Rs 4,417 | Rs 9.40L |
  | **Total** | | | **Rs 5,532** | |

- Estimate Rs 21.52L, including Rs 0.58L AliceBlue extra and Rs 0.90L option buying. Margin used should
  be about Rs 20.6L. Spare Rs 1.79L against the new Rs 1.5L target. SENSEX has 6 days left, so near
  hedges are expensive (±2800 = Rs 11k). Considered and rejected: next-week hedges give Rs 15-60k
  *less* margin benefit (calendar-spread SPAN) at 3-6x the premium.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-11-02 (Mon) - NIFTY expires tomorrow (03 Nov), SENSEX 05 Nov

**Setup:** 7 lots each, NRML. Funds Rs 22.84L. VIX 15.51.

**Decisions**
- `hedge_buyer.py --plan` at 09:19, pre-open prices (NIFTY 23,257, SENSEX 75,233):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0324250CE + NIFTY26N0322250PE (±1000) | 455 | 1.00 + 2.70 | Rs 1,684 | Rs 8.88L |
  | SENSEX26N0580000CE + SENSEX26N0570400PE (±4800) | 140 | 2.45 + 8.15 | Rs 1,484 | Rs 11.00L |
  | **Total** | | | **Rs 3,168** | |

- Estimate Rs 21.16L, including Rs 0.58L AliceBlue extra and Rs 0.70L option buying. Margin used should
  be about Rs 20.5L. Spare Rs 1.67L against a Rs 1.5L target.
- Cheap extra buffer if wanted: NIFTY ±800 (Rs 2,502, +Rs 818) frees Rs 0.93L more. SENSEX ±3000
  (Rs 4,935, +Rs 3,451) frees Rs 2.57L more.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|
| 09:45 | Rs 20.12L | - | - | - | First hour, 7 lots each. Rs 0.4L under the Rs 20.5L estimate. About Rs 2.7L free on Rs 22.83L funds (Rs 2.0L if option buying fires). |
| 13:45 | Rs 20.80L | - | - | - | Up Rs 0.68L since 09:45 (spot/IV drift or re-entries). About Rs 2.0L free. |

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-11-02 (Mon) - NIFTY expires 03 Nov (tomorrow), SENSEX 05 Nov

**Setup:** 7 lots each, NRML. Funds Rs 22.83L. VIX 15.54.

**Decisions**
- `hedge_buyer.py` buy at 09:37 (NIFTY 23,288, SENSEX 75,347), filled 09:38:

  | Leg | Qty | Fill | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY03NOV26C24350 + NIFTY03NOV26P22250 (±1050) | 455 | 0.80 + 2.85 | Rs 1,661 | Rs 9.13L |
  | SENSEX26N0580000CE + SENSEX26N0570600PE (±4700) | 140 | 2.80 + 10.05 | Rs 1,799 | Rs 10.87L |
  | **Total** | | | **Rs 3,460** | |

- Estimate Rs 21.27L, including Rs 0.58L AliceBlue extra and Rs 0.69L option buying. Spare Rs 1.56L
  against the Rs 1.5L target. NIFTY expires tomorrow, so its hedge could sit closer (±1050).

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-11-04 (Wed) - SENSEX expires tomorrow (05 Nov), NIFTY on the 09 Nov weekly

**Setup:** 7 lots each, NRML. Funds Rs 22.98L. VIX 16.07.

**Decisions**
- `hedge_buyer.py --plan` at 09:28 (NIFTY ATM 23,900, SENSEX ATM 77,300):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0925350CE + NIFTY26N0922450PE (±1450) | 455 | 3.10 + 6.65 | Rs 4,436 | Rs 11.14L |
  | SENSEX26N0580500CE + SENSEX26N0574100PE (±3200) | 140 | 4.15 + 12.70 | Rs 2,359 | Rs 9.12L |
  | **Total** | | | **Rs 6,795** | |

- Estimate Rs 21.44L, including Rs 0.58L AliceBlue extra and Rs 0.59L option buying. Margin used should
  be about Rs 20.85L. Spare Rs 1.55L against a Rs 1.54L target.
- Extra buffer if wanted: NIFTY ±1300 (Rs 5,596, +Rs 1,160) frees Rs 0.69L more. SENSEX ±2800
  (Rs 3,388, +Rs 1,029) frees Rs 0.58L more.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-11-05 (Thu) - SENSEX expiry day, NIFTY on the 09 Nov weekly

**Setup:** 7 lots each, NRML. Funds Rs 22.88L (cash Rs 6.60L + collateral Rs 16.68L - Rs 0.40L used). VIX 15.18.
AliceBlue shows COALINDIA-EQ +100 CNC @ 398.05 (Rs 39,805) as a day buy, which is the Rs 0.40L used. GOLDBEES
is not in positions.

**Decisions**
- `hedge_buyer.py --plan` at 09:22 (NIFTY 24,344, SENSEX 78,695, both about 1.8-1.9% above yesterday's ATM):

  | Leg | Qty | Ask | Cost | Margin |
  |---|---|---|---|---|
  | NIFTY26N0925400CE + NIFTY26N0923300PE (±1050) | 455 | 5.80 + 7.95 | Rs 6,256 | Rs 9.33L |
  | SENSEX26N0580300CE + SENSEX26N0577100PE (±1600) | 140 | 10.45 + 8.55 | Rs 2,660 | Rs 11.02L |
  | **Total** | | | **Rs 8,916** | |

- Estimate Rs 21.32L, including Rs 0.58L AliceBlue extra and Rs 0.38L option buying. Margin used should
  be about Rs 20.94L. Free about Rs 1.85L after the hedge premium, about Rs 1.47L if option buying fires.
  Target Rs 1.5L.
- **Bought at 09:38 by the server run:** NIFTY 25450CE @ 5.05 + 23250PE @ 7.75 x455 (Rs 5,824), SENSEX 80200CE @ 9.50
  + 77200PE @ 7.95 x140 (Rs 2,443). Total **Rs 8,267**. Straddles: NIFTY 24400, SENSEX 78800.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|
| 09:45 | Rs 21.3L (SPAN 7.56L + exp 13.27L + about 0.45L AliceBlue extra) | 15.17 | ~24,400 | ~78,800 | User's figure. API utilizedMargin Rs 21.76L also includes Coal India Rs 0.40L and hedge premium Rs 0.08L. Rs 0.36L over the Rs 20.94L estimate. Exposure is exactly 2% NIFTY + 4% SENSEX, no extra charged. Rs 1.52L free, about Rs 1.14L if option buying fires. |

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.

---

## 2026-11-06 (Fri) - NIFTY expires 09 Nov, SENSEX on the new 12 Nov weekly

**Setup:** 7 lots each, NRML. Cash Rs 5.71L + collateral Rs 16.71L = Rs 22.42L before hedges (Rs 23.28L yesterday).
VIX 16.66 at 09:38, up from a 15.58 previous close.

**Decisions**
- **Bought at 09:38 by the server run, Rs 18,899:**

  | Leg | Qty | Fill | Cost |
  |---|---|---|---|
  | NIFTY 25400CE + 23700PE (about ±850) | 455 | 6.50 + 12.95 | Rs 8,850 |
  | SENSEX 84100CE + 75100PE (about ±4500) | 140 | 28.48 + 43.30 | Rs 10,049 |

- Highest spend so far; user queried it. Server plan at 09:37 (`logs/hedge_buyer_2026-11-06.log`): funds Rs 22.44L,
  VIX 16.60, spare target Rs 1.80L (Rs 1.5L yesterday), option-buying reserve Rs 1.07L (Rs 0.38L yesterday), quoted
  Rs 17,885. SENSEX has 6 days left, and nothing past ±4700 met the volume filter, so SENSEX alone cost Rs 8.6k or more.
- Fills cost Rs 1,014 over the quote: the PE asks jumped about 15% between the 09:37:28 plan and the 09:38 order
  (NIFTY 11.15 -> 12.85, SENSEX 38.45 -> 42.75).
- Re-check of all 09:37 candidates: Rs 17,885 was the cheapest set meeting the target. Cheaper sets needed less buffer
  (before hedge premium, total room including the reserve): Rs 2.5L = Rs 15,792, Rs 2.0L = Rs 13,975, Rs 1.5L = Rs 12,770.
  A Rs 10k cost cap would have left today about Rs 2.3L over funds, so it is not a safe fix.
- 09:39 estimate with these hedges held: about Rs 19.46L margin used, about Rs 2.77L free, about Rs 1.69L if option
  buying fires.
- User's point after entry: only Rs 19.72L used, so the hedges were over-bought. Agreed: the pick was cheapest only for
  the Rs 2.87L the program held back, and Rs 2.72L actually sat free. Proposed: cap the option-buying reserve at Rs 70k
  in `build_plan` (would have picked NIFTY ±950 + SENSEX ±4500, Rs 15,792 quoted). Not applied, awaiting user's go-ahead.

**Hourly margin / VIX**

| Time | Margin used | VIX | NIFTY | SENSEX | Notes |
|---|---|---|---|---|---|
| 09:45 | Rs 19.73L (SPAN 9.94L + exp 8.94L + hedge premium 0.19L + about 0.66L AliceBlue extra) | 16.57 | 24,587 | 79,689 | SPAN + exposure Rs 18.88L matches the plan's Rs 18.89L. Rs 2.72L free, about Rs 1.65L if option buying fires. |
| 11:15 | Rs 6.32L (incl. hedge premium) | 17.03 | 24,615 | 79,764 | SENSEX halted 09:57 on the daily loss limit, -115.28 pts = Rs -16,140: CE stopped at 910 after the ladder widened its stop to 30% (-209.71 pts), PE closed at +94.43. SENSEX hedges (Rs 10,049) now protect nothing. NIFTY about -29 pts (Rs -13,182) against its 40 pt limit. Rs 16.12L free. |

**End of day** (to fill in): hedge sell-back value, net hedge cost, straddle day P&L.
