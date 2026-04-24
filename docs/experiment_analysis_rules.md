# Experiment Analysis Rules & Knowledge

Rules and conventions accumulated from building payment experiment analyses.
Follow these when creating or modifying experiment notebooks.

---

## Data Sources

### Assignment Table
Use `production.experimentation_product.assignment` for experiment assignments.
Do **not** use `external_statsig.exposures`.

### Join Keys
When joining `production.dwh.dim_customer` to `production.dwh.fact_customer_to_visitor`,
use `customer_id_anon` (not `customer_id`).

```sql
JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
```

### Bot & Reseller Filtering
Always apply both filters:

1. **Left anti join** `filtered_customers` — excludes ticket resellers, partners, and internal users
   via `dim_customer.is_filtered_ticket_reseller_partner_or_internal = 1`
2. **Inner join** `production.experimentation_product.fact_non_bot_visitors` — keeps only non-bot visitors

---

## Experiment Configuration

### Platform-Specific Start Dates
Each experiment may have a different start date per platform. Always filter assignments
using per-experiment dates, not a single global date.

```sql
WHERE (
    (experiment_hash = '{IOS_HASH}' AND date >= '{IOS_START}')
    OR (experiment_hash = '{ANDROID_HASH}' AND date >= '{ANDROID_START}')
)
```

### Assignment Deduplication
Take the first assignment per visitor per experiment:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY visitor_id, experiment_hash ORDER BY timestamp
) = 1
```

---

## Metric Definitions

### Conversion Rate
- **Formula:** `visitors_booked / visitors_on_payment_page`
- **Base population:** Visitors who reached the payment page (`MobileAppPaymentOptionsLoaded`),
  **not** all assigned visitors. Many assigned visitors drop off before reaching the payment page.

### Initiation Rate
- **Formula:** `visitors_initiated / visitors_on_payment_page`
- **Events that count as initiation:**
  - `MobileAppUITap` where `ui.target = 'payment'`
  - `UISubmit` where `ui.id = 'submit-payment'`
  - `MobileAppPaymentSubmit`
- **Do NOT** use `InitiatePaymentAction` — it does not capture all mobile payment flows.

### Payment Success Rate
- **Formula:** `visitors_booked / visitors_initiated`
- This is a **visitor-level** metric. Failed attempts are invisible if the visitor eventually books.

### Save Card Opt-in Rate
- **Formula:** `opted_in / credit_card_submitters`
- **Denominator:** Only submitters whose `payment_method` is `creditcard` or `payment_card`.
  Do not include PayPal, Apple Pay, Google Pay, etc. — they don't have a save-card checkbox.
- **Numerator:** `save_card_consent` in (`true`, `1`, `yes`) **AND** payment method is credit card.

### Stored Card Adoption Rate
- **Formula:** `used_stored_card / all_submitters`
- A visitor used a stored card when `stored_card_reference` is non-empty.
- The `is_stored_card` field does **not** exist in `MobileAppPaymentSubmit` — do not use it.
- When the `payment_method` field in `MobileAppPaymentSubmit` equals `"storedcard"`,
  the `stored_card_reference` field contains values like `visa_0928`.

### Cart Success Rate (Cart SR)
- **Formula:** `successful_carts / distinct_carts`
- Aggregate by **distinct `shopping_cart_id`**: one row per cart, success = `MAX(is_shopping_cart_successful)`.
- Do **not** calculate per attempt row — that inflates the denominator with retries.

### Customer Attempt Success Rate
- **Formula:** `successful_customer_attempts / distinct_customer_attempts`
- Count attempts by **distinct `payment_provider_reference`** to get one row per customer attempt.
- Exclude system-level retries (multiple rows per `payment_provider_reference`).
- Deduplicate at query time:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.payment_provider_reference
    ORDER BY p.payment_attempt_timestamp
) = 1
```

### Payment Method Mix
- Show `storedcard` as a **separate payment method** (based on `stored_card_reference` being non-empty).
- Always segment by with/without previously stored cards to show the shift clearly.
- In the `with_stored_cards` test group, storedcard cannibalizes credit card and wallet methods.
- In the `without_stored_cards` segment, the mix should be nearly identical between control and test.

---

## Known Issues & Gotchas

### Android `save_card_consent` Bug
The Android app **hardcodes `save_card_consent = true`** as a default value in the
`MobileAppPaymentSubmit` event payload for all credit card payments, **including control users**.
The control group never sees the save-card checkbox, but the backend trusts the flag and stores
cards anyway.

**Evidence:**
- Android control: `save_card_consent = true` for 72,705 visitors, `false` for **0** visitors
- Android test: `save_card_consent = true` for 58,401, `false` for 6,925
- iOS control: field is absent (correct behavior)

**Impact:**
- Android adoption metric is invalid — the ~42-47% control "adoption" is an artifact
- The Android experiment is contaminated — control users also get stored cards
- iOS is clean

### Payment Method Mix Shift & Simpson's Paradox
Stored cards shift volume from high-SR methods (Apple Pay ~98.7%, PayPal ~99%) to lower-SR
credit cards (~93%). This causes overall attempt-level Cart SR / Customer Attempt SR to appear
negative even when no individual payment method got worse.

At the **visitor level**, success rates are virtually identical because failed attempts are
absorbed by retries.

### Multi-Cart Visitors
Visitors commonly have multiple shopping carts (repeat purchases). This is real behavior,
not duplication. Average of ~1.9 customer attempts per visitor comes from multiple carts,
not retries within a single cart.

---

## Event Reference

| Event | Key Fields | Notes |
|-------|------------|-------|
| `MobileAppPaymentOptionsLoaded` | `stored_card_references_list`, `payment_methods_list`, `payment_method_preselected` | First occurrence per visitor = segment assignment |
| `MobileAppPaymentSubmit` | `save_card_consent`, `stored_card_reference`, `payment_method` | `is_stored_card` does NOT exist |
| `BookAction` | — | Booking confirmation |
| `MobileAppUITap` | `ui.target` | Use `target = 'payment'` for initiation |
| `UISubmit` | `ui.id` | Use `id = 'submit-payment'` for initiation |
| `InitiatePaymentAction` | — | Do NOT use for mobile initiation |

## Payment Attempt Table

**Table:** `production.payments.fact_payment_attempt`

| Column | Usage |
|--------|-------|
| `payment_provider_reference` | Unique per customer attempt — use for deduplication |
| `shopping_cart_id` | Unique per cart — use for Cart SR |
| `is_customer_attempt_successful` | Success flag per customer attempt |
| `is_shopping_cart_successful` | Success flag per cart |
| `customer_attempt_rank` | Rank within customer attempts (1 = first) |
| `payment_method` | `payment_card`, `apple_pay`, `google_pay`, `paypal`, etc. |
