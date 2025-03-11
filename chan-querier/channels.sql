CREATE TABLE
    IF NOT EXISTS telegram.channels (
        id BIGINT NOT NULL,
        source_channel_id BIGINT NOT NULL,
        parent_channel_id BIGINT NOT NULL,
        access_hash BIGINT NOT NULL,
        data_owner VARCHAR(255),
        title VARCHAR(255),
        username VARCHAR(255),
        language_code VARCHAR(20),
        search_id VARCHAR(255),
        keyword_id VARCHAR(255),
        keyword VARCHAR(255),
        query_id VARCHAR(255),
        query_date TIMESTAMP,
        nr_participants INT DEFAULT 0,
        nr_recommended INT DEFAULT 0,
        distance_from_core INT DEFAULT 0,
        messages_last_queried_at TIMESTAMP WITH TIME ZONE,
        last_queried_message_id INT DEFAULT 0,
        document_count INT DEFAULT 0,
        message_count INT DEFAULT 0,
        gif_count INT DEFAULT 0,
        music_count INT DEFAULT 0,
        photo_count INT DEFAULT 0,
        url_count INT DEFAULT 0,
        video_count INT DEFAULT 0,
        voice_count INT DEFAULT 0,
        collection_priority NUMERIC(10, 9)
    );

CREATE INDEX telegram_channels_id_index ON telegram.channels (id);

CREATE INDEX telegram_channels_id_access_index ON telegram.channels (id, access_hash);