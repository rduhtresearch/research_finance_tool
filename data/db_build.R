# -----------------------------
# 01_build_rules_db_AH.R
# Build + seed DuckDB rules DB for Scenarios A–H
# -----------------------------

# 1) DB packages
library(DBI)
library(duckdb)

# 2) Where the DB file lives
DB_PATH <- "finance_rules_AH.duckdb"

# 3) Connect (creates file if missing)
con <- dbConnect(duckdb::duckdb(), dbdir = DB_PATH, read_only = FALSE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

# 4) Helper to run SQL quickly
exec_sql <- function(sql) dbExecute(con, sql)

# -----------------------------
# 5) Drop tables to rebuild cleanly (repeatable dev)
# -----------------------------
exec_sql("DROP TABLE IF EXISTS rulesets;")
exec_sql("DROP TABLE IF EXISTS posting_line_types;")
exec_sql("DROP TABLE IF EXISTS dist_rules;")
exec_sql("DROP TABLE IF EXISTS amount_map;")
exec_sql("DROP TABLE IF EXISTS routing_rules;")
exec_sql("DROP TABLE IF EXISTS provider_orgs;")

# -----------------------------
# 6) Create tables (prod-shaped but minimal)
# -----------------------------

# 6.1) Ruleset/version container
exec_sql("
CREATE TABLE rulesets (
  ruleset_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  notes TEXT
);
")

# 6.2) Provider org list (expand later)
exec_sql("
CREATE TABLE provider_orgs (
  provider_org TEXT PRIMARY KEY
);
")

# 6.3) Posting line types (stable IDs)
exec_sql("
CREATE TABLE posting_line_types (
  posting_line_type_id TEXT PRIMARY KEY,
  label TEXT NOT NULL
);
")

# 6.4) dist_rules: which posting lines exist for scenario + row_category (+ optional condition)
exec_sql("
CREATE TABLE dist_rules (
  dist_rule_id TEXT PRIMARY KEY,
  ruleset_id TEXT NOT NULL,

  scenario_id TEXT NOT NULL,           -- A..H (we'll keep it as text)
  row_category TEXT NOT NULL,          -- INVESTIGATION or BASELINE

  condition_field TEXT,               -- e.g. is_medic
  condition_op TEXT,                  -- '='
  condition_value TEXT,               -- 'TRUE' or 'FALSE'

  posting_line_type_id TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,

  notes TEXT,

  FOREIGN KEY (ruleset_id) REFERENCES rulesets(ruleset_id),
  FOREIGN KEY (posting_line_type_id) REFERENCES posting_line_types(posting_line_type_id)
);
")

# 6.5) amount_map: math parameters per posting line type
# We keep this simple: amount = AC * mff * base_mult * split_mult
# - base_mult handles 1.0, 0.2, 0.7 etc.
# - split_mult handles 0.5/0.25/0.4/0.6 etc.
exec_sql("
CREATE TABLE amount_map (
  posting_line_type_id TEXT PRIMARY KEY,
  base_mult DOUBLE NOT NULL,
  split_mult DOUBLE NOT NULL,
  applies_to_row_category TEXT NOT NULL,   -- 'BASELINE', 'INVESTIGATION', or 'BOTH'
  notes TEXT,
  FOREIGN KEY (posting_line_type_id) REFERENCES posting_line_types(posting_line_type_id)
);
")

# 6.6) routing_rules: which destination bucket each line goes to, per scenario (+ optional condition)
# Destination bucket is NOT final cost centre code yet; it's a semantic target.
exec_sql("
CREATE TABLE routing_rules (
  routing_rule_id TEXT PRIMARY KEY,
  ruleset_id TEXT NOT NULL,

  scenario_id TEXT NOT NULL,

  condition_field TEXT,
  condition_op TEXT,
  condition_value TEXT,

  posting_line_type_id TEXT NOT NULL,
  destination_bucket TEXT NOT NULL,       -- DEST_PROVIDER / DEST_RD / DEST_SUPPORT / DEST_TRUST_OH / DEST_PI_ORG

  priority INTEGER NOT NULL DEFAULT 100,
  notes TEXT,

  FOREIGN KEY (ruleset_id) REFERENCES rulesets(ruleset_id),
  FOREIGN KEY (posting_line_type_id) REFERENCES posting_line_types(posting_line_type_id)
);
")

# -----------------------------
# 7) Seed base reference data
# -----------------------------

# 7.1) ruleset
exec_sql("
INSERT INTO rulesets (ruleset_id, name, version, notes)
VALUES ('COMM_AH_V1', 'Commercial Rules A–H', 'v1', 'A–H scenarios; MFF fixed at runtime param for MVP');
")

# 7.2) provider orgs (expand later)
exec_sql("
INSERT INTO provider_orgs (provider_org) VALUES
  ('RDUHT'),
  ('CRF'),
  ('DPT'),
  ('UoE');
")

# 7.3) posting line types
exec_sql("
INSERT INTO posting_line_types (posting_line_type_id, label) VALUES
  ('DIRECT', 'Direct Cost'),
  ('DIRECT_40_PI', 'Direct Cost 40% (PI)'),
  ('DIRECT_60_TEAM', 'Direct Cost 60% (Delivery/Team)'),
  ('CAPACITY_RD', 'Capacity (R&D)'),
  ('INDIRECT_50_DELIVERY', 'Indirect 50% (Delivery/Support)'),
  ('INDIRECT_25_TRUST', 'Indirect 25% (Trust Overhead)'),
  ('INDIRECT_25_PI', 'Indirect 25% (PI)');
")

# -----------------------------
# 8) Seed amount_map (math once, reused everywhere)
# amount = AC * mff * base_mult * split_mult
# -----------------------------

# DIRECT = AC * mff
exec_sql("
INSERT INTO amount_map VALUES
  ('DIRECT', 1.0, 1.0, 'BOTH', 'AC * mff');
")

# CAPACITY_RD = AC * 0.2 * mff
exec_sql("
INSERT INTO amount_map VALUES
  ('CAPACITY_RD', 0.2, 1.0, 'BOTH', 'AC * 0.2 * mff');
")

# Indirect splits apply only to BASELINE (Procedure) rows
# Indirect pot = AC * 0.7 * mff (not posted)
# Then split 50/25/25
exec_sql("
INSERT INTO amount_map VALUES
  ('INDIRECT_50_DELIVERY', 0.7, 0.5, 'BASELINE', 'AC * 0.7 * mff * 0.5');
")
exec_sql("
INSERT INTO amount_map VALUES
  ('INDIRECT_25_TRUST', 0.7, 0.25, 'BASELINE', 'AC * 0.7 * mff * 0.25');
")
exec_sql("
INSERT INTO amount_map VALUES
  ('INDIRECT_25_PI', 0.7, 0.25, 'BASELINE', 'AC * 0.7 * mff * 0.25');
")

# TRD medic split applies only to BASELINE rows (per your decision: procedures only)
exec_sql("
INSERT INTO amount_map VALUES
  ('DIRECT_40_PI', 1.0, 0.4, 'BASELINE', 'AC * mff * 0.4 (TRD medic split)');
")
exec_sql("
INSERT INTO amount_map VALUES
  ('DIRECT_60_TEAM', 1.0, 0.6, 'BASELINE', 'AC * mff * 0.6 (TRD medic split)');
")

# -----------------------------
# 9) Seed dist_rules (what posting lines exist)
# We implement A–G now; H typically needs a TRD exception flag. We'll include H as "like G" for MVP.
# You can split H into H_EXTERNAL / H_TRD later by adding a condition_field like delivered_by_trd.
# -----------------------------

# Helper to insert dist rules
insert_dist_rule <- function(id, scenario, row_category, posting_line, priority,
                             condition_field = NA, condition_op = NA, condition_value = NA, notes = NA) {
  dbExecute(con, "
    INSERT INTO dist_rules
      (dist_rule_id, ruleset_id, scenario_id, row_category,
       condition_field, condition_op, condition_value,
       posting_line_type_id, priority, notes)
    VALUES (?, 'COMM_AH_V1', ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    id, scenario, row_category,
    condition_field, condition_op, condition_value,
    posting_line, priority, notes
  ))
}

# 9.1) Convenience vectors
baseline_std <- c("DIRECT", "CAPACITY_RD", "INDIRECT_50_DELIVERY", "INDIRECT_25_TRUST", "INDIRECT_25_PI")
invest_std <- c("DIRECT", "CAPACITY_RD")

# 9.2) Scenarios that behave like A in terms of "which lines exist":
# A, C, E, G, H  (routing differs; G/H are external but still show splits per your choice)
like_A <- c("A", "C", "E", "G", "H")

for (sc in like_A) {
  # BASELINE standard lines
  pr <- 10
  for (pl in baseline_std) {
    insert_dist_rule(paste0(sc, "_BASE_", pl), sc, "BASELINE", pl, pr)
    pr <- pr + 10
  }
  # INVESTIGATION standard lines (no indirect)
  pr <- 10
  for (pl in invest_std) {
    insert_dist_rule(paste0(sc, "_INV_", pl), sc, "INVESTIGATION", pl, pr)
    pr <- pr + 10
  }
}

# 9.3) TRD scenarios: B, D, F
# Non-medic baseline -> standard baseline
# Medic baseline -> replace DIRECT with DIRECT_40_PI + DIRECT_60_TEAM
# Investigation -> standard investigation (no TRD split)
trd_scenarios <- c("B", "D", "F")

for (sc in trd_scenarios) {
  # BASELINE non-medic: standard baseline lines
  pr <- 20
  for (pl in baseline_std) {
    insert_dist_rule(paste0(sc, "_BASE_NONMED_", pl), sc, "BASELINE", pl, pr,
      condition_field = "is_medic", condition_op = "=", condition_value = "FALSE",
      notes = "TRD scenario: non-medic baseline uses standard direct"
    )
    pr <- pr + 10
  }

  # BASELINE medic: TRD split + capacity + indirect lines (no DIRECT)
  insert_dist_rule(paste0(sc, "_BASE_MED_DIRECT40"), sc, "BASELINE", "DIRECT_40_PI", 5,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE",
    notes = "TRD medic: 40% direct to PI"
  )

  insert_dist_rule(paste0(sc, "_BASE_MED_DIRECT60"), sc, "BASELINE", "DIRECT_60_TEAM", 6,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE",
    notes = "TRD medic: 60% direct to team"
  )

  # Capacity + indirect splits still exist for medic baseline
  insert_dist_rule(paste0(sc, "_BASE_MED_CAP"), sc, "BASELINE", "CAPACITY_RD", 20,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE"
  )

  insert_dist_rule(paste0(sc, "_BASE_MED_I50"), sc, "BASELINE", "INDIRECT_50_DELIVERY", 30,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE"
  )
  insert_dist_rule(paste0(sc, "_BASE_MED_I25T"), sc, "BASELINE", "INDIRECT_25_TRUST", 40,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE"
  )
  insert_dist_rule(paste0(sc, "_BASE_MED_I25P"), sc, "BASELINE", "INDIRECT_25_PI", 50,
    condition_field = "is_medic", condition_op = "=", condition_value = "TRUE"
  )

  # INVESTIGATION (all): standard investigation lines
  insert_dist_rule(paste0(sc, "_INV_DIRECT"), sc, "INVESTIGATION", "DIRECT", 10)
  insert_dist_rule(paste0(sc, "_INV_CAP"), sc, "INVESTIGATION", "CAPACITY_RD", 20)
}

# -----------------------------
# 10) Seed routing_rules (destination buckets)
# We apply your decisions:
# - Scenario A: INDIRECT_50 goes to DEST_SUPPORT (CRDT 50007 or CRF); TRUST 25 to DEST_TRUST_OH; PI 25 to DEST_PI_ORG
# - Scenario G/H external: all indirect splits go to DEST_PROVIDER (external org) but remain separate lines
# We'll handle provider_org specifics at runtime (DEST_PROVIDER resolves to provider_org value).
# -----------------------------

insert_routing <- function(id, scenario, posting_line, dest_bucket, priority,
                           condition_field = NA, condition_op = NA, condition_value = NA, notes = NA) {
  dbExecute(con, "
    INSERT INTO routing_rules
      (routing_rule_id, ruleset_id, scenario_id,
       condition_field, condition_op, condition_value,
       posting_line_type_id, destination_bucket, priority, notes)
    VALUES (?, 'COMM_AH_V1', ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    id, scenario,
    condition_field, condition_op, condition_value,
    posting_line, dest_bucket, priority, notes
  ))
}

# Routing templates:
# Internal-ish scenarios: A, B, C, D, E, F
internal_like <- c("A", "B", "C", "D", "E", "F")

for (sc in internal_like) {
  insert_routing(paste0(sc, "_R_DIRECT"), sc, "DIRECT", "DEST_PROVIDER", 10)
  insert_routing(paste0(sc, "_R_D40"), sc, "DIRECT_40_PI", "DEST_PI_ORG", 10)
  insert_routing(paste0(sc, "_R_D60"), sc, "DIRECT_60_TEAM", "DEST_SUPPORT", 10) # team/support bucket
  insert_routing(paste0(sc, "_R_CAP"), sc, "CAPACITY_RD", "DEST_RD", 10)

  # Indirect splits
  insert_routing(paste0(sc, "_R_I50"), sc, "INDIRECT_50_DELIVERY", "DEST_SUPPORT", 10,
    notes = "50% indirect to support bucket (50007 or CRF)"
  )
  insert_routing(paste0(sc, "_R_I25T"), sc, "INDIRECT_25_TRUST", "DEST_TRUST_OH", 10)
  insert_routing(paste0(sc, "_R_I25P"), sc, "INDIRECT_25_PI", "DEST_PI_ORG", 10)
}

# External scenarios: G, H
external_like <- c("G", "H")
for (sc in external_like) {
  insert_routing(paste0(sc, "_R_DIRECT"), sc, "DIRECT", "DEST_PROVIDER", 10)
  insert_routing(paste0(sc, "_R_CAP"), sc, "CAPACITY_RD", "DEST_RD", 10)

  # All indirect splits route to external provider for visibility (your Q2=A)
  insert_routing(paste0(sc, "_R_I50"), sc, "INDIRECT_50_DELIVERY", "DEST_PROVIDER", 10)
  insert_routing(paste0(sc, "_R_I25T"), sc, "INDIRECT_25_TRUST", "DEST_PROVIDER", 10)
  insert_routing(paste0(sc, "_R_I25P"), sc, "INDIRECT_25_PI", "DEST_PROVIDER", 10)

  # TRD split lines unlikely in external scenarios for MVP; if needed for H_TRD later, add new conditional routing.
}

# -----------------------------
# 11) Print a quick audit so you can see what we seeded
# -----------------------------
cat("\n✅ Built and seeded DB:", DB_PATH, "\n")

cat("\nPosting line types:\n")
print(dbGetQuery(con, "SELECT * FROM posting_line_types ORDER BY posting_line_type_id;"))

cat("\nDist rules count by scenario/category:\n")
print(dbGetQuery(con, "
  SELECT scenario_id, row_category, COUNT(*) AS n
  FROM dist_rules
  GROUP BY scenario_id, row_category
  ORDER BY scenario_id, row_category;
"))

cat("\nRouting rules count by scenario:\n")
print(dbGetQuery(con, "
  SELECT scenario_id, COUNT(*) AS n
  FROM routing_rules
  GROUP BY scenario_id
  ORDER BY scenario_id;
"))
