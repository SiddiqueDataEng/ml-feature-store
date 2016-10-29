-- Feature Store Metadata Schema (PostgreSQL)

-- Feature Registry Table
CREATE TABLE IF NOT EXISTS feature_registry (
    feature_id SERIAL PRIMARY KEY,
    feature_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    feature_type VARCHAR(50) NOT NULL, -- numerical, categorical, text, embedding
    entity VARCHAR(100) NOT NULL, -- user, product, transaction, etc.
    source_table VARCHAR(255),
    transformation TEXT,
    version VARCHAR(20) DEFAULT '1.0',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- Feature Tags
CREATE TABLE IF NOT EXISTS feature_tags (
    tag_id SERIAL PRIMARY KEY,
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    tag_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature Groups
CREATE TABLE IF NOT EXISTS feature_groups (
    group_id SERIAL PRIMARY KEY,
    group_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    entity VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature Group Membership
CREATE TABLE IF NOT EXISTS feature_group_members (
    membership_id SERIAL PRIMARY KEY,
    group_id INTEGER REFERENCES feature_groups(group_id),
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(group_id, feature_id)
);

-- Feature Statistics
CREATE TABLE IF NOT EXISTS feature_statistics (
    stat_id SERIAL PRIMARY KEY,
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    stat_date DATE NOT NULL,
    record_count BIGINT,
    null_count BIGINT,
    mean_value DOUBLE PRECISION,
    stddev_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    percentile_25 DOUBLE PRECISION,
    percentile_50 DOUBLE PRECISION,
    percentile_75 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feature_id, stat_date)
);

-- Feature Drift Monitoring
CREATE TABLE IF NOT EXISTS feature_drift (
    drift_id SERIAL PRIMARY KEY,
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    reference_date DATE NOT NULL,
    current_date DATE NOT NULL,
    mean_drift DOUBLE PRECISION,
    std_drift DOUBLE PRECISION,
    ks_statistic DOUBLE PRECISION,
    drift_detected BOOLEAN,
    alert_sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature Lineage
CREATE TABLE IF NOT EXISTS feature_lineage (
    lineage_id SERIAL PRIMARY KEY,
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    source_table VARCHAR(255),
    source_column VARCHAR(255),
    transformation_logic TEXT,
    upstream_features TEXT[], -- Array of upstream feature names
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature Usage Tracking
CREATE TABLE IF NOT EXISTS feature_usage (
    usage_id SERIAL PRIMARY KEY,
    feature_id INTEGER REFERENCES feature_registry(feature_id),
    model_name VARCHAR(255),
    usage_date DATE NOT NULL,
    request_count BIGINT DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feature_id, model_name, usage_date)
);

-- Indexes
CREATE INDEX idx_feature_name ON feature_registry(feature_name);
CREATE INDEX idx_feature_entity ON feature_registry(entity);
CREATE INDEX idx_feature_active ON feature_registry(is_active);
CREATE INDEX idx_feature_tags_feature ON feature_tags(feature_id);
CREATE INDEX idx_feature_stats_date ON feature_statistics(stat_date);
CREATE INDEX idx_feature_drift_date ON feature_drift(current_date);
CREATE INDEX idx_feature_usage_date ON feature_usage(usage_date);

-- Views
CREATE OR REPLACE VIEW vw_feature_summary AS
SELECT 
    fr.feature_id,
    fr.feature_name,
    fr.description,
    fr.feature_type,
    fr.entity,
    fr.version,
    fg.group_name,
    ARRAY_AGG(DISTINCT ft.tag_name) as tags,
    fs.record_count,
    fs.mean_value,
    fs.stddev_value,
    fr.created_at
FROM feature_registry fr
LEFT JOIN feature_group_members fgm ON fr.feature_id = fgm.feature_id
LEFT JOIN feature_groups fg ON fgm.group_id = fg.group_id
LEFT JOIN feature_tags ft ON fr.feature_id = ft.feature_id
LEFT JOIN LATERAL (
    SELECT * FROM feature_statistics 
    WHERE feature_id = fr.feature_id 
    ORDER BY stat_date DESC LIMIT 1
) fs ON TRUE
WHERE fr.is_active = TRUE
GROUP BY fr.feature_id, fr.feature_name, fr.description, fr.feature_type, 
         fr.entity, fr.version, fg.group_name, fs.record_count, 
         fs.mean_value, fs.stddev_value, fr.created_at;
