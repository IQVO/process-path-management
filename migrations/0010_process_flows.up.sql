-- ADR 0042: process flows, the ordered stages work passes through.
--
-- process_flows holds the aggregate (one row per flow, version = the event
-- version: it starts at 1 and grows by one per accepted change);
-- process_flow_stages holds its ordered stages. Save replaces the stages
-- wholesale, so the child table never keeps a stage a later revision removed.
--
-- A path appears at most once in a flow (UNIQUE flow_id, path_id). There is
-- deliberately NO foreign key from a stage to process_paths: the stage's
-- path must be ACTIVE, which a foreign key cannot say; the use case checks
-- it under the path's row lock (ADR 0028), as for a CPT schedule.
CREATE TABLE process_flows (
    flow_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    version    INTEGER NOT NULL CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE process_flow_stages (
    flow_id      TEXT NOT NULL REFERENCES process_flows (flow_id) ON DELETE CASCADE,
    position     INTEGER NOT NULL CHECK (position >= 0),
    path_id      TEXT NOT NULL,
    process_type TEXT NOT NULL CHECK (process_type IN ('PICK', 'REBIN', 'PACK', 'SLAM', 'DISPATCH', 'ARRIVAL', 'RELOCATE', 'REPLENISH')),
    PRIMARY KEY (flow_id, position),
    UNIQUE (flow_id, path_id)
);

-- DeactivatePath asks "which ACTIVE flows have a stage on this path".
CREATE INDEX idx_process_flow_stages_path ON process_flow_stages (path_id);

-- The default view and the startup republish read the ACTIVE flows only.
CREATE INDEX idx_process_flows_active ON process_flows (flow_id) WHERE status = 'ACTIVE';
