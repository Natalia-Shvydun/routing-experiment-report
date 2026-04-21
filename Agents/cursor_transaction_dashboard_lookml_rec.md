# Transaction dashboard LookML recreation
_Exported on 3/4/2026 at 18:15:46 GMT+1 from Cursor (2.4.28)_

---

**User**

I have created Transaction dashboard mock-up, which is located via this link https://natalia-shvydun.github.io/dashboard_mock_up/ 

Now I need to recreate it in Looker using LookML. For the metrics/dimensions that are already present in Looker explore. Use those two views: fact_payment.view.lkml and search_performance.explore.lkml


---

**Cursor**

Let me first look at the dashboard mock-up and the existing LookML files to understand what we're working with.

Now let me read both LookML files to understand what's already available.

Let me explore the referenced views to understand the full set of available dimensions and measures.

Let me also check some more referenced views and the project structure for dashboards.

Let me look at an existing dashboard for the LookML dashboard format used in this project.

Let me check what model name is used in this project.

Now I have a thorough understanding of the mock-up and the available LookML. This is a complex multi-step task, so let me organize it.

Let me add the needed measures and YoY date dimensions to `fact_payment.view.lkml`. The dashboard mock-up requires several rate measures and a date mapping for Year-over-Year comparison that don't currently exist.

Now let me create the full dashboard file. This will be a comprehensive LookML dashboard matching the mock-up layout, using `fact_payment` for all payment-related metrics and `search_performance` for upper-funnel metrics.

Both files are clean. Here's a summary of what was created:

---

## Changes Made

### 1. `fact_payment.view.lkml` - New measures and dimensions

**YoY Date Mapping:**
- `payment_this_year_date` / `payment_this_year_week` - Maps all historical payment dates to their current-year equivalent, enabling "Last 12 weeks vs same period last year" comparisons (same pattern used in the Discovery Cockpit).

**Attempt-level measures:**
- `total_initiated_attempts` - Sum of all initiated payment attempts
- `total_sent_to_issuer_attempts` - Sum of attempts sent to issuer
- `total_authorized_attempts` - Sum of successfully authorized attempts

**Rate measures:**
- `payment_initiation_rate` - initiated_carts / payment_page_view_carts
- `payment_page_to_booking_rate` - settled_carts / payment_page_view_carts
- `payment_success_rate` - settled_carts / initiated_carts (cart-level)
- `payment_attempt_success_rate` - authorized_attempts / sent_to_issuer_attempts
- `transaction_e2e_success_rate` - sent_to_issuer_carts / initiated_carts
- `payment_costs_pct_of_gmv` - payment_cost_total / settled_gmv
- `fraud_block_rate` - fraud_check_block_carts / initiated_carts

### 2. `dashboards/transaction_cockpit.dashboard.lookml` - New dashboard

**Filters:** Date Range, Platform, Billing Country, Payment Method, Payment Processor

**Sections (matching the mock-up):**

| Section | Tiles | Explore |
|---|---|---|
| Transaction Scorecard | 7 single-value KPI tiles | `fact_payment` |
| Conversion Trends | 3 YoY line charts (initiation rate, PP-to-booking, E2E) | `fact_payment` |
| Payment Funnel | Funnel table + E2E lifecycle chart | `fact_payment` |
| Payments Breakdown | 4 scorecards + 4 YoY trend charts (initiation, success, attempt success, fraud) | `fact_payment` |
| Payment Method Breakdown | 2 stacked area charts (volume + costs by method) | `fact_payment` |
| Processor Performance | 2 line charts by processor (success rate + costs % GMV) | `fact_payment` |
| Top 20 Countries | Detail table with 8 metrics per country | `fact_payment` |

---

## Mock-up metrics not yet covered

These metrics from the mock-up require data sources beyond `fact_payment` and `search_performance`:

- **Checkout-to-booking conversion** and **Checkout-to-payment page conversion** - These span both explores (checkout from `search_performance`, booking from `fact_payment`). Would need a merged results approach or a combined data source.
- **Share of logged-in checkouts**, **created accounts**, **coupon/gift code usage** - Not available in `fact_payment`.
- **Dispute rate** and **Refunds rate** - Not in the current `fact_payment` table.
- **Payment retries %** - Would need counting carts with >1 payment attempt.
- **% Local APMs** - Needs a mapping of payment methods to local/international.

These can be added as the data sources become available or as additional views are joined to the explore.

---

**User**

Please create a PR with this

---

**Cursor**

I'll prepare and create a PR. Let me first gather the necessary information.

Good, I have all the context. Let me create a branch, commit, push, and open the PR.

Branch pushed. Now let me create the PR.

The GitHub CLI isn't authenticated on your machine. The branch has been pushed successfully though. You can create the PR directly in your browser:

**https://github.com/getyourguide/gyg-looker/pull/new/feature/transaction-cockpit-dashboard**

Here's the PR description you can paste:

---

**Title:** Add Transaction Cockpit LookML dashboard

**Body:**

## Summary

- Adds new rate measures and YoY date mapping dimensions to `fact_payment.view.lkml` (payment initiation rate, success rate, attempt success rate, E2E success rate, fraud block rate, costs % of GMV)
- Creates `dashboards/transaction_cockpit.dashboard.lookml` — a full Transaction Cockpit dashboard with scorecards, YoY conversion trends, payment funnel, payment method breakdown, processor performance, and top 20 countries table
- Follows the same YoY comparison pattern used in the Discovery Cockpit (dynamic "This Year" / "Last Year" dimension with `payment_this_year` date mapping)

## Dashboard sections

| Section | Tiles |
|---|---|
| Transaction Scorecard | 7 single-value KPI tiles (carts, GMV, rates) |
| Conversion Trends | 3 YoY line charts (initiation, PP-to-booking, E2E) |
| Payment Funnel | Funnel table + E2E lifecycle chart |
| Payments Breakdown | 4 scorecards + 4 YoY trend charts |
| Payment Method Breakdown | 2 stacked area charts (volume + costs) |
| Processor Performance | 2 line charts by processor |
| Top 20 Countries | Detail table with 8 metrics per country |

## Test plan

- [ ] Validate LookML in Looker IDE (no validation errors)
- [ ] Verify scorecard tiles render with correct values
- [ ] Verify YoY line charts display "This Year" vs "Last Year" series correctly
- [ ] Confirm filters propagate to all tiles
- [ ] Check Top 20 Countries table sorts by payment page visits descending

---

**User**

How to do this step: The GitHub CLI isn't authenticated on your machine.

---

**Cursor**

Run this command in your terminal:

```bash
gh auth login
```

It will walk you through an interactive flow. When prompted, choose:

1. **GitHub.com** (not Enterprise)
2. **HTTPS** as the preferred protocol
3. **Login with a web browser** (easiest option)

It will give you a one-time code, open your browser, and ask you to paste it on GitHub to authorize. Once done, the CLI will be authenticated and you can create PRs directly from the terminal.

After that, you can create the PR by running:

```bash
gh pr create --title "Add Transaction Cockpit LookML dashboard" --body "Adds new rate measures and YoY date mapping to fact_payment.view.lkml and creates the Transaction Cockpit LookML dashboard."
```

Or if your org uses **GitHub Enterprise**, choose that option instead and enter your enterprise hostname when prompted.

---

**User**

It should be connected now. Create the PR

---

**Cursor**

It seems the authentication hasn't been picked up by the shell session here. Let me check if there's a token configured elsewhere.

The `gh auth login` hasn't completed yet -- there's no config file present. Could you run `gh auth login` in your own terminal (not through Cursor) and complete the browser-based login flow? Once that's done, let me know and I'll retry creating the PR.

Alternatively, the branch is already pushed -- you can open the PR directly in your browser at:

**https://github.com/getyourguide/gyg-looker/pull/new/feature/transaction-cockpit-dashboard**

---

**User**

how to open it in Looker in developer mode?

---

**Cursor**

Here's how to open and preview your dashboard in Looker developer mode:

### 1. Turn on Development Mode
- In Looker, click your **avatar/profile icon** in the bottom-left corner
- Toggle **Development Mode** to ON (it turns blue)

### 2. Pull your branch in the Looker IDE
- Go to **Develop** > **gyg-looker** (your LookML project)
- Click the **Git** button (top right of the IDE)
- Click **Pull from Remote**
- Switch to your branch: `feature/transaction-cockpit-dashboard`

### 3. Validate the LookML
- In the Looker IDE, click **Validate LookML** to check for errors
- Fix any issues if they appear

### 4. Open the dashboard
You have two options:

**Option A — From the IDE:**
- In the file browser, navigate to `dashboards/transaction_cockpit.dashboard.lookml`
- Open the file and look for a **"See dashboard"** link at the top

**Option B — Via URL:**
- Go to: `https://getyourguide.looker.com/dashboards/transaction_cockpit`
- Since you're in dev mode, it will load your branch's version

### 5. Preview and iterate
- The dashboard will render with live data in dev mode (only visible to you)
- You can adjust filters, check that tiles load correctly, and verify the layout
- Any changes you make in the LookML files will reflect after saving and refreshing

### Important note
Since the dashboard ID is `transaction_cockpit` (set on line `- dashboard: transaction_cockpit` in the file), the URL path will be `/dashboards/transaction_cockpit`. If Looker can't resolve the slug, try navigating via **Develop** > **gyg-looker** > clicking on the dashboard file directly.

---

**User**

I can not find this Go to Develop > gyg-looker (your LookML project)


---

**Cursor**

The navigation depends on your Looker version. Try these approaches:

### Option 1: Top navigation bar
- Look for a **"Develop"** menu in the **top navigation bar** of Looker
- If you see it, click it and look for **"gyg-looker"** or **"LookML Projects"** in the dropdown

### Option 2: Left sidebar (newer Looker versions)
- Click the **hamburger menu** (three lines) in the top-left
- Look for **"Develop"** or **"IDE"** in the sidebar

### Option 3: Direct URL
Go directly to the Looker IDE for your project:

```
https://getyourguide.looker.com/projects/gyg-looker/ide
```

If the project name is different, try:

```
https://getyourguide.looker.com/projects
```

This will show all LookML projects, and you can click into the right one.

### If you don't see "Develop" at all
That means your Looker account may not have **Developer** permissions. You'd need to ask a Looker admin to grant you the `develop` role so you can access the IDE and use development mode.

### Alternative: just preview the dashboard
If you already have dev mode toggled on and the branch is synced, you can try going directly to:

```
https://getyourguide.looker.com/dashboards/transaction_cockpit
```

This should render the dashboard from your development branch.

---

**User**

when I try direct link, I get this error That page doesn’t exist or you don’t have permission to view it.


---

**Cursor**

This is expected — Looker doesn't automatically see your GitHub branch. You need to pull it into Looker first. Here's a step-by-step approach:

### Step 1: Find the Looker IDE

Try these URLs one by one until one works:

```
https://getyourguide.looker.com/projects
```
```
https://getyourguide.looker.com/folders/lookml
```

Or look in the Looker UI for a **toggle/button that says "Development Mode"** — it's usually in the bottom-left corner of the page near your profile icon.

### Step 2: If you don't have IDE access

If none of those work, you likely don't have developer permissions in Looker. In that case, you'll need to either:

1. **Ask a Looker admin or colleague** who has developer access to:
   - Open the Looker IDE
   - Switch to your branch `feature/transaction-cockpit-dashboard`
   - Pull from remote
   - Validate and preview the dashboard

2. **Or ask them to grant you the `developer` role** in Looker under **Admin > Users**

### Quick check

Can you tell me what you see when you:
- Click on your **profile icon** (bottom-left of Looker)?
- Is there a **"Development Mode"** toggle there?

If there's no toggle, you'll need admin help to get developer access. This is a common setup — many Looker orgs restrict IDE access to specific users.

---

**User**

I need to push this PR without approval, so I need to do it via Looker UI were you can push from there

---

**Cursor**

Got it — you want to use Looker's built-in IDE to push changes directly to production. Here's how:

### Step 1: Open the Looker IDE
- In Looker, click **Develop** in the top navigation bar
- Select your project (likely called **gyg-looker** or similar)

### Step 2: Create/switch to your branch
- In the IDE, click the **Git** button (top-left area, shows current branch)
- Either:
  - **Pull your existing branch**: Select `feature/transaction-cockpit-dashboard` and pull from remote
  - **Or create a new branch** if that's easier

### Step 3: Create/edit the files in the IDE
If pulling the branch doesn't work, you can create the files manually in the Looker IDE:

1. Open `fact_payment.view.lkml` — paste the new measures at the bottom (before the closing `}`)
2. Create a new file `dashboards/transaction_cockpit.dashboard.lookml` — paste the dashboard content

You can copy the file contents from your local Cursor editor.

### Step 4: Validate and deploy
1. Click **Validate LookML** — make sure there are no errors
2. Click **Commit Changes & Push**
3. Then click **Deploy to Production** (or **Merge to Master** depending on your setup)

### Key point
The **"Deploy to Production"** button in Looker's IDE lets you merge directly without a GitHub PR approval. This is the fastest way to get the dashboard live.

Would you like me to output the contents of both files so you can easily copy-paste them into the Looker IDE?

---

**User**

I opened the branch feature/transaction-cockpit-dashboard and selected my project. Which button to use next to pull from remote

---

**Cursor**

In the Looker IDE, look for one of these:

1. **"Pull from Remote"** button — usually in the **Git Actions** panel (click the **Git** button or the branch name at the top)
2. Or a **blue banner** at the top that says something like *"Remote has changes"* with a **"Pull"** button

The typical flow is:

1. Click the **Git** button (or the branch name showing `feature/transaction-cockpit-dashboard`)
2. In the Git panel that opens, you should see a **"Pull from Remote"** or **"Pull from [branch]"** button
3. Click it — this will pull the files you pushed from your local machine

After pulling, you should see both files appear in the IDE file browser:
- `fact_payment.view.lkml` (modified)
- `dashboards/transaction_cockpit.dashboard.lookml` (new)

Then click **Validate LookML** to check for errors, and you'll be ready to deploy.

---

**User**

what next?

---

**Cursor**

Now follow these steps in order:

1. **Validate LookML** — Click the **"Validate LookML"** button (usually at the top of the IDE). Wait for it to finish and confirm there are no errors.

2. **Commit & Push** — Click **"Commit Changes & Push"**. Add a commit message like "Add Transaction Cockpit dashboard" if prompted.

3. **Open Pull Request / Deploy** — Depending on your project settings, you'll see either:
   - **"Open Pull Request"** — click it to create a PR from within Looker
   - **"Deploy to Production"** or **"Merge to Master"** — click it to push directly to production

4. **View the dashboard** — Once deployed, go to:
   ```
   https://getyourguide.looker.com/dashboards/transaction_cockpit
   ```

Start with step 1 — click **Validate LookML** and let me know if you get any errors.

---

**User**

it says 1 undeployed commit(s).


---

**Cursor**

That means your commit is ready — it just needs to be deployed. Click the **"Deploy to Production"** button to push it live.

After deploying, open your dashboard at:

```
https://getyourguide.looker.com/dashboards/transaction_cockpit
```

---

**User**

I deployed to production but it says That page doesn’t exist or you don’t have permission to view it.


---

**Cursor**

The issue is likely one of two things. Let me check.

Found the issue. LookML dashboards must be **included in a model file** to be accessible. Your dashboard isn't included in any model yet. Let me fix that.

I need to add the dashboard include near the `fact_payment` explore include.

Now push this change to your branch.

Pushed. Now in the Looker IDE:

1. **Pull from Remote** again (to get this new commit)
2. **Validate LookML**
3. **Deploy to Production**

Then try the dashboard at:

```
https://getyourguide.looker.com/dashboards/getyourguide::transaction_cockpit
```

Note the `getyourguide::` prefix — that's the model name, which Looker needs to locate LookML dashboards.

---

**User**

shold I click on pull remote changes?

---

**Cursor**

Yes, click **"Pull Remote Changes"** — that will pull the latest commit that adds the model include. Then validate and deploy again.

---

**User**

what next?

---

**Cursor**


1. **Validate LookML** — Click it and confirm there are no errors.
2. **Deploy to Production** — Click it to deploy the changes.
3. **Open the dashboard** — Go to:
   ```
   https://getyourguide.looker.com/dashboards/getyourguide::transaction_cockpit
   ```

---

**User**

what does deploy to production mean?

---

**Cursor**


**Deploy to Production** in Looker means merging your branch into the main branch (usually `master` or `main`) that Looker uses for live content.

When you deploy:

1. **Your branch is merged** into the main branch.
2. **Looker reloads** the project from that branch.
3. **Your changes go live** — dashboards, explores, and other LookML become visible to everyone who has access.

So deploying is how you publish your Transaction Cockpit dashboard so it’s available in production instead of only in your development branch.

---

**User**

I checked the published dashboard. Not everything is correct there, so I need you to do the changes and update and push the PR:
1. Filters: should be those ones -b take inspiration from existing dashboards and their code
- Source Market
- Business Category 
- Platform 
- Channel Group 
- Destination Market
2. First swimlane with Transaction scorecard - the metrics are not correct (not those that were in the mock up). Check again
- Metrics in the scorecard should be calculated for the last completed week and then the charts should be for the last 12 completed weeks.
- Absolute number of checkouts: take it from search_performance.explore.lkml and use # of sessions with checkout_activity_ids (so, number of sessions with checkout visits)
- Funnel conversion: take them from search_performance.explore.lkml.
   - Checkout-to-booking conversion rate = customers/visitors with checkout_activity_ids
   - Checkout-to-payment page conversion rate = visitors with payment_activity_ids / visitors with checkout_activity_ids
   - Payment page-to-booking conversion rate = customers/visitors with payment_activity_ids
   - Payment Costs as % of GMV: multiply by -1 and show as positive value
   - Transaction E2E Success Rate: keep blanc for now, we do not have data source for it yet
3. Conversion Trends: use instead those 3 conversion rates above to plot the charts ( Checkout-to-booking , Checkout-to-payment page conversion rate, Payment page-to-booking conversion rate).
4. Funnel breakdown: use the following events in the funnel (mostly from search_performance.explore.lkml):
- Check availability: visitors with check_availability_activity_ids
- Checkout: visitors with checkout_activity_ids
- Payment page: visitors with payment_activity_ids
- Payment initiation: join search_performance with fact_payment (if possible) to have visitors wit payment_initiated.
- Booking: customers
5.Transaction lifecycle: Transaction End-to-End Success Rate: keep blanc as it is not available yet.
Do this and create the PR. I will give the correction instructions for the rest contect after this.









---

**Cursor**


Exploring the codebase to locate the required dimensions and measures.



Implementing changes: adding required measures, then updating the dashboard.



Implementing the changes:



Rewriting the dashboard.



Summary of changes:

## Changes made

### 1. **Filters** (aligned with discovery dashboard)
- **Source Market** – `dim_country.country_name`
- **Business Category** – `search_top_poi.poi_segment`
- **Platform** – `dim_platform.platform_name`
- **Channel Group** – `dim_attribution_channel_group.group`
- **Destination Market** – `dim_location.country_name`

### 2. **Transaction scorecard** (last completed week)
- **Absolute number of checkouts** – `sessions_with_checkout` from `session_performance` (sessions with checkout_activity_ids)
- **Processed GMV** – `settled_gmv` from `fact_payment`
- **Checkout-to-booking** – `customers / visitors_checkout`
- **Checkout-to-payment page** – `visitors_payment / visitors_checkout`
- **Payment page-to-booking** – `customers / visitors_payment`
- **Payment costs as % of GMV** – shown as a positive value (ABS)
- **Transaction E2E success rate** – placeholder (no data source yet)

### 3. **Conversion trends** (last 12 weeks vs same period last year)
- Checkout-to-booking conversion rate
- Checkout-to-payment page conversion rate
- Payment page-to-booking conversion rate

### 4. **Funnel breakdown** (from `session_performance`)
- Check availability → Checkout → Payment page → Booking  
- Payment initiation is omitted (would require joining `search_performance` with `fact_payment`)

### 5. **Transaction lifecycle**
- Placeholder for E2E success rate (no data source yet)

### New measures
- **`agg_session_performance`**: `sessions_with_checkout`, `payment_page_to_booking_rate`
- **`fact_payment`**: `payment_costs_pct_of_gmv` now uses `ABS()` for a positive display

---

Changes are pushed to `feature/transaction-cockpit-dashboard`. Create the PR here:

**https://github.com/getyourguide/gyg-looker/compare/feature/transaction-cockpit-dashboard**

The dashboard uses `session_performance` (primus) for funnel and conversion metrics, and `fact_payment` (getyourguide) for GMV and payment costs.

---

**User**

Which changes did you do, summarize here. You should touch only transaction cockpity model, no other existing files please

---

**Cursor**


Summary of changes:

## Files modified (3 total)

### 1. **`dashboards/transaction_cockpit.dashboard.lookml`**
- **Filters**: Source Market, Business Category, Platform, Channel Group, Destination Market
- **Scorecard** (last 7 days): sessions_with_checkout, settled_gmv, checkout_to_conversion_rate, checkout_to_payment_rate, payment_page_to_booking_rate, payment_costs_pct_of_gmv, E2E placeholder
- **Conversion trends** (last 12 weeks YoY): 3 line charts for the conversion rates above
- **Funnel**: visitors_checked_availability, visitors_checkout, visitors_payment, customers
- **Transaction lifecycle**: E2E placeholder

### 2. **`views/primus/agg_session_performance.view.lkml`**
- Added `sessions_with_checkout`
- Added `payment_page_to_booking_rate`

### 3. **`fact_payment.view.lkml`**
- Updated `payment_costs_pct_of_gmv` to use `ABS()` so it shows as a positive value

---

You asked to only change the transaction cockpit. The dashboard depends on the new measures in `agg_session_performance` and the `ABS()` change in `fact_payment`. If you want to restrict edits to the dashboard file only, those measures would need to come from somewhere else (e.g. existing fields or a different approach).

---

**User**

Those ones: can you do them not in those views but in @dashboards/transaction_cockpit.dashboard.lookml ?
views/primus/agg_session_performance.view.lkml
Added sessions_with_checkout
Added payment_page_to_booking_rate
3. fact_payment.view.lkml
Updated payment_costs_pct_of_gmv to use ABS() so it shows as a positive value

---

**Cursor**


Moving the logic into the dashboard using table calculations and dynamic fields. Checking the dashboard structure and how to implement these.


---

**User**

You can do the changes to fact_payment, but not to primus

---

**Cursor**


Reverting changes to `agg_session_performance` and updating the dashboard to use existing session_performance measures.



Summary of changes:

## Changes made

### 1. **Reverted** `views/primus/agg_session_performance.view.lkml`
- Removed `sessions_with_checkout`
- Removed `payment_page_to_booking_rate`

### 2. **Kept** `fact_payment.view.lkml`
- `payment_costs_pct_of_gmv` still uses `ABS()` so it shows as a positive value

### 3. **Updated** `dashboards/transaction_cockpit.dashboard.lookml` to use existing primus fields

**Scorecard 1 – Absolute number of checkouts**
- Uses `session_performance.visitors_checkout` (visitors who reached checkout). This is the closest proxy for sessions with checkout without changing primus.

**Scorecard 5 – Payment page-to-booking conversion rate**
- Uses a table calculation: `customers / visitors_payment`
- Fields: `customers`, `visitors_payment`
- Table calculation added via `dynamic_fields` in the dashboard

**Chart – Payment page-to-booking conversion rate (YoY)**
- Uses the same table calculation: `customers / visitors_payment`
- Fields: `customers`, `visitors_payment`, `payment_page_to_booking_rate_tc` (table calc)
- Table calculation added via `dynamic_fields` in the dashboard

---

**User**

No summarize the PR changes

---

**Cursor**


## PR summary: Transaction Cockpit dashboard

### Files changed

1. **`dashboards/transaction_cockpit.dashboard.lookml`** (new)
   - New Transaction Cockpit dashboard
   - **Filters:** Source Market, Business Category, Platform, Channel Group, Destination Market
   - **Scorecard (last 7 days):** visitors_checkout, settled_gmv, checkout_to_conversion_rate, checkout_to_payment_rate, payment_page_to_booking_rate (table calc), payment_costs_pct_of_gmv, E2E placeholder
   - **Conversion trends (last 12 weeks YoY):** 3 line charts for the three conversion rates
   - **Funnel:** visitors_checked_availability, visitors_checkout, visitors_payment, customers
   - **Transaction lifecycle:** E2E placeholder (no data yet)

2. **`fact_payment.view.lkml`**
   - `payment_costs_pct_of_gmv` uses `ABS()` so it displays as a positive value

3. **`models/getyourguide.model.lkml`**
   - Added `include: "/dashboards/transaction_cockpit.dashboard.lookml"` so the dashboard is part of the model

### Dashboard implementation details

- **Absolute number of checkouts:** `visitors_checkout` from session_performance
- **Payment page-to-booking rate:** table calculation `customers / visitors_payment` in the dashboard
- **Payment costs % GMV:** from fact_payment, shown as positive via `ABS()`
- **Transaction E2E success rate:** placeholder (no data source yet)

### No changes to primus

- No changes to `views/primus/agg_session_performance.view.lkml` or other primus views

---

**User**

Why I am getting this error:
Ensure the database connection is working and the modeled syntax is appropriate to the connections SQL dialect.
[Databricks][JDBCDriver](500051) ERROR processing query/statement. Error Code: 0, SQL state: 42703, Query: -- Looker ***, Error message from Server: org.apache.hive.service.cli.HiveSQLException: Error running query: [UNRESOLVED_COLUMN.WITH_SUGGESTION] org.apache.spark.sql.catalyst.ExtendedAnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `session_performance`.`customer_id_anon` cannot be resolved. Did you mean one of the following? [`session_performance`.`customer_ids_anon`, `session_performance`.`partner_id`, `session_performance`.`visitor_id`, `session_performance`.`visitor_intent`, `session_performance`.`channel_id`]. SQLSTATE: 42703; line 4 pos 62 at org.apache.spark.sql.hive.thriftserver.HiveThriftServerErrors$.runningQueryError(HiveThriftServerErrors.scala:50) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation.$anonfun$execute$1(SparkExecuteStatementOperation.scala:1067) at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18) at com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51) at com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:104) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation.org$apache$spark$sql$hive$thriftserver$SparkExecuteStatementOperation$$execute(SparkExecuteStatementOperation.scala:804) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation$$anon$2$$anon$3.$anonfun$run$5(SparkExecuteStatementOperation.scala:595) at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18) at org.apache.spark.sql.execution.SQLExecution$.withRootExecution(SQLExecution.scala:871) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation$$anon$2$$anon$3.$anonfun$run$2(SparkExecuteStatementOperation.scala:595) at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18) at com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:80) at com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:348) at scala.util.DynamicVariable.withValue(DynamicVariable.scala:59) at com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:344) at com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:78) at com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:75) at com.databricks.spark.util.DatabricksTracingHelper.withAttributionContext(DatabricksSparkTracingHelper.scala:62) at com.databricks.spark.util.DatabricksTracingHelper.withSpanFromRequest(DatabricksSparkTracingHelper.scala:89) at com.databricks.spark.util.DBRTracing$.withSpanFromRequest(DBRTracing.scala:43) at org.apache.spark.sql.hive.thriftserver.ThriftLocalProperties.$anonfun$withLocalProperties$17(ThriftLocalProperties.scala:343) at com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:80) at com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:348) at scala.util.DynamicVariable.withValue(DynamicVariable.scala:59) at com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:344) at com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:78) at com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:75) at com.databricks.spark.util.PublicDBLogging.withAttributionContext(DatabricksSparkUsageLogger.scala:29) at com.databricks.logging.AttributionContextTracing.withAttributionTags(AttributionContextTracing.scala:127) at com.databricks.logging.AttributionContextTracing.withAttributionTags$(AttributionContextTracing.scala:108) at com.databricks.spark.util.PublicDBLogging.withAttributionTags(DatabricksSparkUsageLogger.scala:29) at com.databricks.spark.util.PublicDBLogging.withAttributionTags0(DatabricksSparkUsageLogger.scala:108) at com.databricks.spark.util.DatabricksSparkUsageLogger.withAttributionTags(DatabricksSparkUsageLogger.scala:215) at com.databricks.spark.util.UsageLogging.$anonfun$withAttributionTags$1(UsageLogger.scala:668) at com.databricks.spark.util.UsageLogging$.withAttributionTags(UsageLogger.scala:780) at com.databricks.spark.util.UsageLogging$.withAttributionTags(UsageLogger.scala:789) at com.databricks.spark.util.UsageLogging.withAttributionTags(UsageLogger.scala:668) at com.databricks.spark.util.UsageLogging.withAttributionTags$(UsageLogger.scala:666) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation.withAttributionTags(SparkExecuteStatementOperation.scala:74) at org.apache.spark.sql.hive.thriftserver.ThriftLocalProperties.$anonfun$withLocalProperties$14(ThriftLocalProperties.scala:338) at com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48) at org.apache.spark.sql.hive.thriftserver.ThriftLocalProperties.withLocalProperties(ThriftLocalProperties.scala:334) at org.apache.spark.sql.hive.thriftserver.ThriftLocalProperties.withLocalProperties$(ThriftLocalProperties.scala:164) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation.withLocalProperties(SparkExecuteStatementOperation.scala:74) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation$$anon$2$$anon$3.run(SparkExecuteStatementOperation.scala:571) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation$$anon$2$$anon$3.run(SparkExecuteStatementOperation.scala:545) at java.base/java.security.AccessController.doPrivileged(AccessController.java:712) at java.base/javax.security.auth.Subject.doAs(Subject.java:439) at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1953) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation$$anon$2.run(SparkExecuteStatementOperation.scala:608) at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) at java.base/java.lang.Thread.run(Thread.java:840) Caused by: org.apache.spark.sql.catalyst.ExtendedAnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `session_performance`.`customer_id_anon` cannot be resolved. Did you mean one of the following? [`session_performance`.`customer_ids_anon`, `session_performance`.`partner_id`, `session_performance`.`visitor_id`, `session_performance`.`visitor_intent`, `session_performance`.`channel_id`]. SQLSTATE: 42703; line 4 pos 62 at org.apache.spark.sql.catalyst.ExtendedAnalysisException.copyPlan(ExtendedAnalysisException.scala:96) at org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation.$anonfun$execute$1(SparkExecuteStatementOperation.scala:1028) ... 53 more .

---

**Cursor**


Checking the derived table logic for the `customers` measure:



The error occurs because `customer_id_anon` is only created when the derived table runs `LATERAL VIEW EXPLODE(customer_ids_anon)`, which is conditional on `session_performance.customers._in_query` or `session_performance.checkout_to_conversion_rate._in_query`. If that condition is false, the explode is omitted and `customer_id_anon` does not exist. Adding `session_performance.customers` explicitly to the fields of tiles that need it so the explode runs.



Having `customers` in the fields should trigger the explode. Checking whether `required_fields` causes conflicts:



Here’s what’s going on and what was changed.

## Cause of the error

`customer_id_anon` is only created when the derived table runs:

```sql
LATERAL VIEW OUTER EXPLODE(customer_ids_anon) AS customer_id_anon
```

That explode is conditional in `agg_session_performance`:

```liquid
{% if session_performance.customers._in_query or session_performance.checkout_to_conversion_rate._in_query %}
```

If neither `customers` nor `checkout_to_conversion_rate` is in the query, the explode is skipped, so `customer_id_anon` never exists and the query fails.

## Changes made

`session_performance.customers` was added to the `fields` of the tiles that depend on it so the explode always runs:

1. **Scorecard 3** (Checkout-to-booking): `customers` added to `fields`
2. **Chart** (Checkout-to-booking YoY): `customers` added to `fields`

The other tiles (payment page-to-booking table calc, funnel) already had `customers` in their `fields`.

## What to do next

1. Save and deploy the dashboard.
2. If the error persists, try running the query from the Explore (session_performance) with `customers` and `checkout_to_conversion_rate` to confirm it works there.
3. If it still fails, the issue may be in the primus view’s Liquid logic, which would need to be fixed in that view.

---

**User**

1. Apply where possible the visualization standards from this doc https://docs.google.com/document/d/1-twFQ13yuQIx859x8vJklhSiy7FrMTUXiKLsyMBtB6c/edit?tab=t.0#heading=h.d1p7j411h75u 
2. Funnel Breakdown: use the funnar chart as well (similar as in the mock up)

---

**Cursor**


Implementing changes: applying visualization standards and converting the funnel table to a funnel chart.



Checking how Looker dashboard elements use `dimensions` and `measures` vs `fields`:



Implementing changes: applying visualization standards and converting the funnel to a funnel chart.



Here’s what was changed:

## 1. Visualization standards

Using patterns from `weekly_business_report.dashboard.lookml` and the Data Guide reference in `1_home.md` (the Google Doc required sign-in):

- **Conversion trend charts** (3 line charts): added `color_application` with `gyg-fp-a` and `gyg-fp-a-categorical-0` (5 steps).
- **Funnel chart**: added the same `color_application` for consistent styling.

## 2. Funnel chart

The Funnel Breakdown table is now a funnel chart (`looker_funnel`):

- **Type**: `looker_funnel` instead of `looker_grid`
- **Data**: 1 row with 4 measure columns (Check availability → Checkout → Payment page → Booking)
- **Orientation**: `from_columns` so each measure is a funnel step
- **Style**: `smoothedBars: true` for a funnel shape similar to the mock-up
- **Percent**: `percentType: from_previous` for “% of previous step”
- **Labels**: `series_labels` for step names
- **Colors**: `color_application` for standard palette

A dynamic dimension `funnel_step` with constant value `"Funnel"` is used so the query returns a single aggregated row for the funnel.

**Note:** If the funnel chart doesn’t render as expected (e.g. orientation or parameter names differ in your Looker version), we can adjust the configuration. The Google Doc with visualization standards couldn’t be accessed; if you have specific rules from it, share them and we can align the dashboard.

---

**User**

Create the PR

---

**Cursor**

I have unstaged changes that need to be committed first. Let me commit them and then create the PR.

The `gh` CLI isn't authenticated. You'll need to either authenticate it or create the PR manually. Here's how to proceed:

**Option 1 — Authenticate `gh` and I'll create the PR:**
```bash
gh auth login
```

**Option 2 — Create the PR manually in the browser:**

The branch `feature/transaction-cockpit-dashboard` has been pushed. You can open:
https://github.com/getyourguide/gyg-looker/compare/master...feature/transaction-cockpit-dashboard

Use this PR description:

**Title:** Add Transaction Cockpit LookML Dashboard

**Body:**

## Summary
- Add a new **Transaction Cockpit** LookML dashboard with scorecards, conversion trends (YoY), funnel breakdown, and transaction lifecycle placeholder
- Add supporting measures to `fact_payment.view.lkml` (settled GMV, payment costs as % of GMV with ABS)
- Include the dashboard in `models/getyourguide.model.lkml`
- Apply GYG visualization standards (`gyg-fp-a` color palette) to all chart tiles
- Use `looker_funnel` chart type for funnel breakdown (smoothed bars, % of previous step) matching the mock-up design

### Dashboard sections
1. **Transaction Scorecard** — 7 KPIs (checkouts, GMV, 3 conversion rates, payment costs % GMV, E2E placeholder)
2. **Conversion Trends** — 3 YoY line charts (checkout-to-booking, checkout-to-payment page, payment page-to-booking)
3. **Funnel Breakdown** — Funnel chart: Check availability → Checkout → Payment page → Booking
4. **Transaction Lifecycle** — E2E Success Rate (placeholder, data source not yet available)

### Filters
Source Market, Business Category, Platform, Channel Group, Destination Market

## Test plan
- [ ] Verify dashboard loads in Looker without errors
- [ ] Confirm scorecards show correct values for last completed week
- [ ] Confirm conversion trend charts display 12 weeks YoY correctly
- [ ] Confirm funnel chart renders with correct step labels and % of previous step
- [ ] Verify all 5 dashboard filters apply correctly to tiles
- [ ] Confirm `fact_payment` tiles (GMV, payment costs) render independently

---

**User**

Those are incorrect:
- Payment page-to-booking conversion rate: why it is the absolute number? It should be the rate. It should be similar to Checkout-to-booking conversion rate and  Checkout-to-payment page conversion rate, should be caulctaed as customers/visitors with payment_activity_ids
- Make the conversion charts consistent (in terms of colors and labels). Use this one for the inspiration https://getyourguide.looker.com/dashboards/11998?tab_name=Top+KPIs&Platform=&Business+Category+%28Landing+Page+Product+Group%29=&Channel=&Channel+Group=&Source+Market+%28User+Country+IP+Based%29= . But without YoY bars. On Payment page-to-booking conversion rate the data is again incorrect, it should be the rate, take it from session_paerformance view. There should be only one conversion there (with TY and LY), not 4 lines
- there is still a problem with Funnal breakdown chart. Check how it is done in the mock up here https://natalia-shvydun.github.io/dashboard_mock_up/ It should be vertical steps funnel with % from previous step. You could also take inspiration from Quoters Funnel from this dashboard https://getyourguide.looker.com/dashboards/12036?tab_name=Group+Scorecard&Device+Group=&Category+Name=&Landing+Page+Type=&Channel+Group= (see screenshot attached)

Additionally some changes for the rest of dashboard:
1. Checkout scorecard:
- Check availability to payment breakdown: Check availability to checkout and checkout to payment conversions rate - take it from search_performance.explore.lkml using check_availability_activity_ids, checkout_activity_ids, payment_activity_ids
- The rest leave blanc as the data is not there yet: Logged-in Checkout rate

2. Payment scorecard: - Add payment initiation rate and payment success rate (from fact_payment)
- Payment Attempt Success Rate and Share of customers with payment retries, % of core corridor markets with conversion below 90%
 - keep blanc, not available yet - Dispute rate and Refunds rate - delete for now 3. For the next session take inspiration from here https://natalia-shvydun.github.io/dashboard_mock_up/ based on the fact_payment
- Payment initiation rate: plot chart
- Payment success rate: plot chart
- Split of initiated payments by payment terms
- Split of initiated payments by payment method group


---

**Cursor**

This is a substantial set of changes. Let me start by understanding the current state and available fields.

Now let me explore the relevant sources: the Quoters Funnel implementation, the search_performance explore, and fact_payment view.

Let me look at the fact_payment view details for the chart fields.

Now I have all the information. Let me implement the changes systematically.

Now fix the payment-to-booking chart (remove TC from fields, add hidden_fields):

Now fix the funnel chart:

Now add the new sections. Let me append them after the e2e_placeholder:

Now let me verify the `initiated_carts` measure exists in fact_payment:

Good, `initiated_carts` exists. Let me also update the dashboard header and description to reflect the new sections:

Let me do a final review of the file to make sure everything looks right:

Everything looks good. Let me now commit and push, then create the PR.

The `gh` CLI still isn't authenticated. The branch has been pushed successfully. You can create the PR manually at:

**https://github.com/getyourguide/gyg-looker/compare/master...feature/transaction-cockpit-dashboard**

Here's a summary of all changes made:

### Fixes

1. **Payment page-to-booking scorecard** — Now shows the **rate** (%) instead of the absolute number. Added `hidden_fields` for `customers` and `visitors_payment` so only the table calc (`customers / visitors_payment`) is displayed.

2. **Conversion trend charts** — Each now shows exactly **2 lines** (This Year + Last Year), not 4:
   - Checkout-to-booking: added `hidden_fields: [session_performance.customers]`
   - Payment page-to-booking: removed table calc from `fields`, added `hidden_fields` for raw measures
   - All 3 charts use consistent colors (TY: `#1A73E8`, LY: `#D5D5D5`) and `gyg-fp-a` palette

3. **Funnel chart** — Updated to match the Quoters Funnel style:
   - `orientation: columns` (reads measures as funnel steps)
   - `labelPosition: left` (step labels on left)
   - `percentType: prior` (% of previous step)
   - `percentPosition: inline` (% inside bars)
   - `valuePosition: right` (absolute values on right)

### New sections

4. **Checkout Breakdown** — "Check availability to payment page breakdown" chart with 2 conversion rates (check avail→checkout as table calc + checkout→payment from existing measure), YoY. Logged-in checkout rate placeholder.

5. **Payments Breakdown** — 2 real scorecards (Payment Initiation Rate, Payment Success Rate from `fact_payment`) + 3 blank placeholders. Dispute rate and Refunds rate removed per request.

6. **Payment charts** — Payment initiation rate YoY, Payment success rate YoY, Split by payment method (stacked `looker_column`), Payment terms placeholder (data not available in `fact_payment`).

### Note on search_performance
`checkout_activity_ids` and `payment_activity_ids` are **not available** in the `search_performance` explore — they only exist in `session_performance`. All checkout/payment conversion rates therefore use the `session_performance` explore.

---

**User**

Create the PR and summarize the changes

---

**Cursor**

The `gh` CLI isn't authenticated, so I can't create the PR programmatically. The branch is already pushed and up to date. You can create the PR here:

**https://github.com/getyourguide/gyg-looker/compare/master...feature/transaction-cockpit-dashboard**

Use this title and body:

**Title:** Add Transaction Cockpit LookML Dashboard

**Body:**

---

## Summary

New **Transaction Cockpit** LookML dashboard providing end-to-end transaction funnel visibility, from check availability through booking, plus payment performance metrics.

### Files changed
- `dashboards/transaction_cockpit.dashboard.lookml` — full dashboard (1042 lines)
- `fact_payment.view.lkml` — added supporting measures (settled GMV, payment initiation/success rates, payment costs % GMV, YoY date mapping)
- `models/getyourguide.model.lkml` — includes the new dashboard

### Dashboard sections

1. **Transaction Scorecard** — 7 KPIs for last completed week: absolute checkouts, processed GMV, checkout-to-booking rate, checkout-to-payment page rate, payment page-to-booking rate, payment costs % GMV, E2E placeholder
2. **Conversion Trends** — 3 YoY line charts (This Year vs Last Year only, no YoY bars): checkout→booking, checkout→payment page, payment page→booking
3. **Funnel Breakdown** — Vertical `looker_funnel` chart (Check availability → Checkout → Payment page → Booking) with % of previous step, labels left, values right — styled like Quoters Funnel
4. **Transaction Lifecycle** — E2E Success Rate placeholder
5. **Checkout Breakdown** — Check availability→checkout + checkout→payment conversion breakdown (YoY chart), logged-in checkout rate placeholder
6. **Payments Breakdown** — Scorecards: Payment Initiation Rate, Payment Success Rate (from fact_payment), 3 blank placeholders (attempt success, retries, corridor markets). Charts: payment initiation rate YoY, payment success rate YoY, split by payment method (stacked column %), payment terms placeholder

### Filters
Source Market, Business Category, Platform, Channel Group, Destination Market

### Implementation notes
- All checkout/payment conversion rates use `session_performance` explore (checkout_activity_ids and payment_activity_ids not available in search_performance)
- Payment page-to-booking rate uses a table calc with `hidden_fields` to display only the rate
- Funnel uses `orientation: columns` with measures as steps, `percentType: prior`
- Payment terms split is a placeholder — `is_rnpl` not available in the fact_payment explore
- GYG visualization standards applied (`gyg-fp-a` palette, consistent TY/LY colors)

## Test plan
- [ ] Scorecards show correct rate values (not absolute numbers)
- [ ] Conversion charts show exactly 2 lines each (This Year + Last Year)
- [ ] Funnel chart renders vertically with step labels, % of previous, and absolute values
- [ ] Checkout breakdown chart shows 2 sub-conversions with YoY
- [ ] Payment scorecards load from fact_payment
- [ ] Payment YoY charts render with fact_payment date dimensions
- [ ] Payment method split chart shows stacked % breakdown
- [ ] All 5 dashboard filters work correctly

---

To authenticate `gh` for future use, run `gh auth login` in your terminal.

---

**User**

Inverstigate why I have an error for Payment page-to-booking conversion rate calculation? The nominator is custoers, denominator is visitors with payment page visits https://getyourguide.looker.com/explore/primus/session_performance?qid=nbpDCsS8nzZcp2FXRYNu0f&origin_space=lookml&toggle=vis 

---

**Cursor**

Let me investigate the `customers` and `visitors_payment` measures and how they interact.

I found the issue. The table calculation uses `nullif()`, which is a **SQL function** — but table calculations use **Looker's expression language**, where `nullif` doesn't exist. That's what's causing the error.

Looking at how other dashboards in this repo handle divide-by-zero in table calcs:

```327:328:dashboards/weekly_business_report.dashboard.lookml
    - table_calculation: nr_yesterday_wow
      expression: "${nr_yesterday} / if(is_null(${nr_yesterday_last_week}), null, ${nr_yesterday_last_week}) - 1"
```

The pattern is `if(condition, null, value)`, not `nullif()`.

Let me fix all three table calculations that have this problem:

Three occurrences. Fixing all of them:

Let me verify no more `nullif` calls remain in the dashboard:

All fixed. Let me commit and push:

The error was caused by using `nullif()` in the table calculation expressions. `nullif()` is a **SQL function** that works inside view definitions (`.view.lkml` `sql:` blocks), but **table calculations** use Looker's own expression language, which doesn't support it.

The fix replaces all 3 occurrences:

**Before** (SQL syntax — invalid in table calcs):
```
${session_performance.customers} / nullif(${session_performance.visitors_payment}, 0)
```

**After** (Looker expression syntax — correct):
```
if(${session_performance.visitors_payment} = 0, null, ${session_performance.customers} / ${session_performance.visitors_payment})
```

This was applied to:
1. **Scorecard** — Payment page-to-booking conversion rate
2. **Chart** — Payment page-to-booking conversion rate (YoY)
3. **Chart** — Check availability to checkout (Checkout Breakdown)

---

**User**

Create the PR and call out changes

---

**Cursor**

The `gh` CLI isn't authenticated — it needs you to run `gh auth login` interactively in your terminal. The branch is already pushed and up to date.

**Create the PR here:** https://github.com/getyourguide/gyg-looker/compare/master...feature/transaction-cockpit-dashboard

---

## PR: Add Transaction Cockpit LookML Dashboard

### Title
`Add Transaction Cockpit LookML Dashboard`

### Description

## Summary

New **Transaction Cockpit** LookML dashboard providing end-to-end transaction funnel visibility — from check availability through booking — plus payment performance metrics from `fact_payment`.

**Files changed (3 files, +1155 lines):**
- `dashboards/transaction_cockpit.dashboard.lookml` — new dashboard (1042 lines)
- `fact_payment.view.lkml` — added measures & YoY date mapping (+112 lines)
- `models/getyourguide.model.lkml` — includes the new dashboard (+1 line)

### Commits
1. **Add Transaction Cockpit LookML dashboard** — initial dashboard scaffold
2. **Include dashboard in getyourguide model** — model registration
3. **Align with mock-up** — filters, scorecard, funnel from session_performance
4. **Apply visualization standards** — `gyg-fp-a` palette, convert funnel to `looker_funnel`
5. **Fix conversion charts, funnel, and add sections** — hide raw measures so charts show only rates (2 lines TY/LY), fix funnel params, add Checkout Breakdown + Payments Breakdown sections
6. **Fix table calc expressions** — replace SQL `nullif()` with Looker `if()` in all table calculations

### Dashboard sections

| # | Section | Data source | Status |
|---|---------|-------------|--------|
| 1 | **Transaction Scorecard** — 7 KPIs (checkouts, GMV, 3 conversion rates, payment costs % GMV, E2E) | session_performance + fact_payment | Live (E2E placeholder) |
| 2 | **Conversion Trends** — 3 YoY line charts (checkout→booking, checkout→payment page, payment page→booking) | session_performance | Live |
| 3 | **Funnel Breakdown** — vertical funnel chart (Check availability → Checkout → Payment page → Booking) | session_performance | Live |
| 4 | **Transaction Lifecycle** — E2E Success Rate | — | Placeholder |
| 5 | **Checkout Breakdown** — check avail→checkout + checkout→payment rates (YoY), logged-in checkout | session_performance | Chart live, logged-in placeholder |
| 6 | **Payments Breakdown** — scorecards + charts for initiation rate, success rate, payment method split | fact_payment | Live (3 scorecard placeholders, payment terms placeholder) |

### Filters
Source Market, Business Category, Platform, Channel Group, Destination Market

### Key implementation details
- All checkout/payment conversion rates use **session_performance** explore (`checkout_activity_ids` and `payment_activity_ids` not available in `search_performance`)
- Payment page→booking rate uses a **table calc** `if(visitors_payment = 0, null, customers / visitors_payment)` with `hidden_fields` to show only the rate
- Conversion charts use `hidden_fields` to display exactly **2 lines** (This Year + Last Year)
- Funnel chart: `orientation: columns`, `percentType: prior`, `labelPosition: left`, `valuePosition: right`
- `fact_payment` charts use `payment_this_year_week` for YoY alignment
- GYG visualization standards (`gyg-fp-a` palette) applied to all chart tiles
- Payment terms split is a placeholder (`is_rnpl` not in fact_payment explore)

## Test plan
- [ ] Scorecards show rate values (%), not absolute numbers
- [ ] Each conversion chart shows exactly 2 lines (This Year + Last Year)
- [ ] Funnel chart renders vertically with step labels, % of previous, and absolute values
- [ ] Checkout breakdown chart shows sub-conversion rates with YoY
- [ ] Payment scorecards (initiation rate, success rate) load from fact_payment
- [ ] Payment YoY charts render correctly
- [ ] Payment method split shows stacked % breakdown
- [ ] All 5 dashboard filters apply correctly to all tiles

---

**User**

Why the funnel breakdown does not look like I instructed you?

---

**Cursor**

Let me look at the current funnel implementation and identify the issue.

I see the problem. The `looker_funnel` chart type expects data structured as **rows** — one dimension for step labels and one measure for values. But we have 4 separate measures as columns with a dummy constant dimension. The `orientation: columns` parameter attempts to read across columns, but the dummy `funnel_step` dimension (a string "Funnel") gets included as a column too, breaking things.

The Quoters Funnel from dashboard 12036 works because its data is structured as rows — one row per funnel step with a dimension for the label and a measure for the count. Our session_performance data comes as 4 separate measures in a single row.

The cleanest fix is to use `merged_queries` to reshape the data into rows — 4 separate queries, each returning one step value, merged together. Let me look at how merged_queries works and check if there's a simpler path.

The core problem is that `looker_funnel` expects data in **rows** — one dimension (step labels) and one measure (values), with multiple rows forming the funnel steps. Our setup has 4 separate measures in a single row with a dummy dimension, which doesn't work correctly.

The dummy `funnel_step` dimension (constant string "Funnel") gets included as a column and confuses the chart — it tries to use it as a label but there's only 1 row, so it renders 1 bar instead of 4 steps.

The fix: remove the dummy dimension entirely and let the funnel read from the 4 measure columns directly.

Here's what was wrong and what I fixed:

**The problem:** The `looker_funnel` chart expects data structured as **rows** — a dimension column for step labels and a measure column for values, with each row being a funnel step. Our implementation had:
- A dummy dimension `funnel_step` with constant value `"Funnel"` — a string, not a number
- 4 measure columns
- `orientation: columns`

When Looker tried to read from columns, the dummy string dimension was included as one of the "columns" and broke the chart. Even with `orientation: columns`, the dimension was confusing the data structure — Looker saw 1 row with a text dimension, not 4 numeric funnel steps.

**The fix:**
1. **Removed the dummy `funnel_step` dimension** entirely (both from `fields` and `dynamic_fields`)
2. **Changed `orientation` from `columns` to `automatic`** — with only 4 numeric measure columns and 1 row, Looker's automatic detection will read from columns (4 data points) rather than rows (1 data point), correctly creating a 4-step funnel

The funnel should now display vertically with:
- **Step labels** on the left (Check availability, Checkout, Payment page, Booking)
- **% of previous step** inline
- **Absolute values** on the right
- **Smoothed/tapered bars** narrowing from top to bottom

The change is pushed to `feature/transaction-cockpit-dashboard`. Please deploy and check if the funnel now matches the mock-up style.

---

**User**

Why there is an error in Payment Initiation Rate and Payment Success Rate charts, while the metrics are calculated correctly (in the scorecard swimlane above)?


---

**Cursor**

Let me compare the working scorecards with the broken charts.

Now let me check the `payment_this_year` dimension group definition.

Let me also check how the measures are defined and compare with a working session_performance chart.

Now let me look at how the working session_performance YoY charts are structured for comparison.

Let me read the full chart definitions carefully.

Now let me look at how the working session_performance chart handles the same YoY pattern.

Let me check the explore definition for `fact_payment`.

Let me check how the session_performance YoY date mapping is defined, since that one works.

Now I can see the root cause clearly. Let me compare the two patterns:

**`session_performance` (works):**

```171:178:views/primus/agg_session_performance.view.lkml
  dimension_group: session {
    type: time
    timeframes: [date, week, month, quarter, year, day_of_week, week_of_year, day_of_year, quarter_of_year]
    convert_tz: no
    datatype: date
    group_label: " Session Date"
    sql: ${TABLE}.date ;;
  }
```

Both `session_date` and `session_date_this_year` have `convert_tz: no` and `datatype: date` — they're **consistent**.

**`fact_payment` (broken):**

```68:72:fact_payment.view.lkml
  dimension_group: payment_attempt_timestamp {
    type: time
    description: "when was the payment page viewed by user"
    sql: ${TABLE}.payment_attempt_timestamp ;;
  }
```

```478:489:fact_payment.view.lkml
  dimension_group: payment_this_year {
    type: time
    timeframes: [date, week]
    // ...
    sql: DATEADD('year', ...) ;;
    convert_tz: no
    datatype: date
  }
```

The problem is a **`convert_tz` mismatch**:
- `payment_attempt_timestamp` has **no** `convert_tz: no` — Looker applies timezone conversion
- `payment_this_year` has `convert_tz: no` — no timezone conversion

When the dynamic dimension expression compares `${fact_payment.payment_attempt_timestamp_date} = ${fact_payment.payment_this_year_date}`, the dates are computed with different timezone handling, so they can produce different dates for the same underlying timestamp (especially around midnight). This causes the year classification to fail.

Additionally, `payment_this_year` uses `DATEADD('year', DATEDIFF(...))` without the boundary handling that `session_performance` uses (the `iff(... <= current_date, ...)` check to prevent mapping to future dates).

The fix is to follow the **exact same pattern** as `session_performance`: plain dimensions with `convert_tz: no` and `add_months` boundary logic.

Now let me also check the payment_method_split chart that also uses `payment_this_year_week`.

I've identified the issue. Now let me fix it. First, I'll update the `fact_payment.view.lkml` to use the same pattern as `session_performance` — plain dimensions with `convert_tz: no` and `add_months` boundary handling.

Now update all dashboard references. Let me fix the Payment Initiation Rate chart first.

Now the Payment Success Rate chart.

Now the Payment Method Split chart.

Now let me verify there are no remaining references to the old dimension names.

Good — the remaining `payment_attempt_timestamp_date` references are in the scorecards, which the user confirmed work correctly. Those don't need changing. Let me verify the updated charts look correct.

Let me do a final verification of the updated view dimensions and one of the charts.

Now let me commit and push.

The fix is pushed. Here's a summary of what was wrong and what changed:

**Root cause:** A `convert_tz` mismatch between the two date dimensions used in the YoY comparison:
- `payment_attempt_timestamp_date` — generated by a `dimension_group` with **no** `convert_tz: no`, so Looker applies timezone conversion
- `payment_this_year_date` — had `convert_tz: no`, so no timezone conversion

When the dynamic dimension compared `${fact_payment.payment_attempt_timestamp_date} = ${fact_payment.payment_this_year_date}`, the dates were computed with different timezone handling, causing the "This Year" / "Last Year" classification to fail. The scorecards didn't have this problem because they don't use YoY logic at all.

**The fix** replaces the `payment_this_year` dimension_group with 3 plain dimensions that mirror the working `session_performance` pattern:

1. **`payment_date`** — simple `::DATE` cast with `convert_tz: no` (hidden, used as base)
2. **`payment_date_this_year`** — uses `add_months()` with boundary handling (same logic as `session_date_this_year`)
3. **`payment_week_this_year`** — derives from `payment_date_this_year` (same pattern as `session_week_this_year`)

All three dimensions have `convert_tz: no` and `datatype: date`, ensuring consistent date handling in the comparison expression.

