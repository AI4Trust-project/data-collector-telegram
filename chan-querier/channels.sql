CREATE TABLE
    IF NOT EXISTS telegram.channels (
        id BIGINT NOT NULL,
        access_hash BIGINT NOT NULL,
        username VARCHAR(255),
        source_channel_id BIGINT,
        parent_channel_id BIGINT,
        channel_last_queried_at TIMESTAMP WITH TIME ZONE,
        messages_last_queried_at TIMESTAMP WITH TIME ZONE,
        last_queried_message_id INT DEFAULT 0,
        -- priority data
        language_code VARCHAR(20),
        nr_participants INT DEFAULT 0,
        distance_from_core INT DEFAULT 0,
        message_count INT DEFAULT 0,
        collection_priority NUMERIC(10, 9)
        metadata_collection_priority NUMERIC(10, 9)
        -- search info
        search_id VARCHAR(255),
        keyword_id VARCHAR(255),
        keyword VARCHAR(255),
        -- query info
        data_owner VARCHAR(255),
        query_id VARCHAR(255),
        query_date TIMESTAMP,
    );

CREATE INDEX telegram_channels_id_index ON telegram.channels (id);

CREATE INDEX telegram_channels_id_access_index ON telegram.channels (id, access_hash);
