CREATE TABLE
    IF NOT EXISTS telegram.channels_rels (
        source BIGINT,
        source_parent BIGINT,
        destination BIGINT,
        destination_parent BIGINT,
        destination_username VARCHAR(255),
        relation VARCHAR(20),
        nr_messages BIGINT,
        first_discovered TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_discovered TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

CREATE INDEX telegram_channels_rels_idx ON telegram.channels_rels (source);

CREATE INDEX telegram_channels_rels_ridx ON telegram.channels_rels (source, relation);

CREATE INDEX telegram_channels_rels_reidx ON telegram.channels_rels (destination);

CREATE INDEX telegram_channels_rels_reridx ON telegram.channels_rels (destination, relation);

CREATE INDEX telegram_channels_rels_uq ON telegram.channels_rels (source, destination, relation);

CREATE INDEX telegram_channels_rels_uq_un ON telegram.channels_rels (source, destination_username, relation);
