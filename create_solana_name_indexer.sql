CREATE TABLE solana_name_indexer (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    parent_node VARCHAR(66),
    is_tokenized BOOLEAN DEFAULT FALSE,
    nft_owner VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    registration_hash VARCHAR(1024),
    registration_height INT DEFAULT 0,
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(66),
    resolver VARCHAR(66),
    resolved_address VARCHAR(66),
    reverse_address VARCHAR(66),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    texts JSONB default '{}'::jsonb,
    resolved_records JSONB default '{}'::jsonb,
    twitter_handle VARCHAR(255),
    texts_v2 JSONB default '{}'::jsonb,
    CONSTRAINT unique_solana_name_indexer UNIQUE (namenode)
);


CREATE INDEX solana_name_indexer_name_index ON solana_name_indexer (name);
CREATE INDEX solana_name_indexer_label_name_index ON solana_name_indexer (label_name);
CREATE INDEX solana_name_indexer_owner_index ON solana_name_indexer (owner);
CREATE INDEX solana_name_indexer_resolved_index ON solana_name_indexer (resolved_address);
CREATE INDEX solana_name_indexer_reverse_index ON solana_name_indexer (reverse_address);