import datetime
import json
import os
import time
import uuid

import collegram
import nest_asyncio
import psycopg
import psycopg.rows
from kafka import KafkaProducer
from lingua import LanguageDetectorBuilder
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
from telethon.sessions import StringSession

DB_NAME = os.environ.get("DATABASE_NAME")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_HOST = os.environ.get("DATABASE_HOST")
TELEGRAM_OWNER = os.environ["TELEGRAM_OWNER"]

RELS_TABLE = "telegram.channels_rels"
WAIT_INTERVAL = 60 * 60  # 1 hour
DELAY = 10


async def init_context(context):
    # Connect to an existing database
    connection = psycopg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        dbname=DB_NAME,
    )
    connection.autocommit = True
    setattr(context, "connection", connection)

    # start telegram client
    client = TelegramClient(
        StringSession(os.environ["AI4TRUST_TG_SESSION"]),
        os.environ["AI4TRUST_API_ID"],
        os.environ["AI4TRUST_API_HASH"],
        flood_sleep_threshold=24 * 3600,
    )

    await client.start(os.environ["AI4TRUST_PHONE_NUMBER"])
    setattr(context, "client", client)

    # init kafka producer
    broker = os.environ.get("KAFKA_BROKER")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x, default=_iceberg_json_default).encode(
            "utf-8"
        ),
    )
    setattr(context, "producer", producer)

    # language
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()
    setattr(context, "lang_detector", lang_detector)


def _iceberg_json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(value, set):
        return list(value)
    else:
        return repr(value)


def get(cur, table, keys, kwargs):
    fields = [f for f in keys]
    where = " AND ".join("%s=%%s" % pkf for pkf in kwargs)
    where_args = [kwargs[pkf] for pkf in kwargs]
    cur.execute(
        "SELECT %s FROM %s WHERE %s LIMIT 1" % (",".join(fields), table, where),
        where_args,
    )
    r = cur.fetchone()
    if r:
        return dict(zip(fields, r))

    return None


def count(cur, table, kwargs):
    where = " AND ".join("%s=%%s" % pkf for pkf in kwargs)
    where_args = [kwargs[pkf] for pkf in kwargs]
    cur.execute(
        "SELECT COUNT(*) FROM %s WHERE %s" % (table, where),
        where_args,
    )
    r = cur.fetchone()
    if r:
        return r[0]

    return 0


def upsert(cur, table, pk_fields, kwargs):
    # check to see if it already exists
    where = " AND ".join("%s=%%s" % pkf for pkf in pk_fields)
    where_args = [kwargs[pkf] for pkf in pk_fields]
    cur.execute("SELECT COUNT(*) FROM %s WHERE %s LIMIT 1" % (table, where), where_args)
    fields = [f for f in kwargs.keys()]
    if cur.fetchone()[0] > 0:
        set_clause = ", ".join("%s=%%s" % f for f in fields if f not in pk_fields)
        set_args = [kwargs[f] for f in fields if f not in pk_fields]
        cur.execute(
            "UPDATE %s SET %s WHERE %s" % (table, set_clause, where),
            set_args + where_args,
        )
        return False
    else:
        field_placeholders = ["%s"] * len(fields)
        fmt_args = (table, ",".join(fields), ",".join(field_placeholders))
        insert_args = [kwargs[f] for f in fields]
        cur.execute("INSERT INTO %s (%s) VALUES (%s)" % fmt_args, insert_args)
        return True


def upsert_recommended(cur, source, source_parent, dest, dest_parent, query_time):
    cur.execute(
        f"INSERT INTO {RELS_TABLE} (source, source_parent, destination, destination_parent, relation, first_discovered, last_discovered) "
        f" VALUES({source}, {source_parent}, {dest}, {dest_parent},'recommended',%s,%s) "
        " ON CONFLICT(source, destination, relation) "
        f" DO UPDATE SET last_discovered=%s",
        [query_time, query_time, query_time],
    )


def upsert_channel(cur, channel):
    upsert(cur, "telegram.channels", ["id"], channel)


def gen_query_info(query_time=None):
    if query_time is None:
        query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
    return {
        "query_id": str(uuid.uuid4()),
        "query_date": query_time,
        "data_owner": TELEGRAM_OWNER,
    }


def gen_msg_key(row: dict):
    return (
        "+".join(
            str(p)
            for p in [
                row.get("search_id"),
                row["source_channel_id"],
                row["id"],
                row["query_id"],
            ]
        )
        if row.get("search_id") is not None
        else "+".join(
            str(p)
            for p in [
                row["source_channel_id"],
                row["id"],
                row["query_id"],
            ]
        )
    )


def handle_recommended(
    context,
    recommended,
    src_chat_id,
    src_source_channel_id,
    src_parent_id,
    src_distance_from_core,
    query_info,
    base,
):
    rec_log = f"{src_source_channel_id} chat {src_chat_id} recommended {recommended.id}"
    context.logger.debug(f"Process channel {rec_log}")
    connection = context.connection

    with connection.cursor() as cur:
        existing_count = count(
            cur,
            RELS_TABLE,
            {
                "source": src_chat_id,
                "destination": recommended.id,
                "relation": "recommended",
            },
        )

    if existing_count == 0:
        context.logger.debug(f"Record channel {rec_log}")

        with connection.cursor() as cur:
            rec_data = get(
                cur,
                "telegram.channels",
                ["parent_channel_id", "distance_from_core"],
                {"id": recommended.id},
            )
        rec_dist_from_core = src_distance_from_core + 1
        if rec_data is None:
            rec_parent_id = recommended.id
        else:
            rec_parent_id = rec_data["parent_channel_id"] or recommended.id
            rec_dist_from_core = min(rec_dist_from_core, rec_data["distance_from_core"])

        # split recommended into table to obtain an adjacency matrix
        with connection.cursor() as cur:
            upsert_recommended(
                cur,
                src_chat_id,
                src_parent_id,
                recommended.id,
                rec_parent_id,
                query_info["query_date"],
            )

        with connection.cursor() as cur:
            rec_fwds, rec_links, rec_recs = [
                count(
                    cur,
                    RELS_TABLE,
                    {"destination_parent": rec_parent_id, "relation": rel},
                )
                for rel in ("forwarded", "linked", "recommended")
            ]

        rec_priority = collegram.channels.get_centrality_score(
            rec_dist_from_core, rec_fwds, rec_recs, rec_links
        )
        upsert_data = {
            "id": recommended.id,
            "access_hash": recommended.access_hash,
            "username": recommended.username,
            "distance_from_core": rec_dist_from_core,
            "metadata_collection_priority": rec_priority,
            **base,
        }

        with connection.cursor() as cur:
            upsert(cur, "telegram.channels", ["id"], upsert_data)


def get_base_dict(source_data):
    base = {
        "search_id": source_data.get("search_id", None),
        "keyword_id": source_data.get("keyword_id", None),
        "keyword": source_data.get("keyword", None),
    }
    return base


def get_full_metadata(
    context,
    channel_id,
    access_hash,
    channel_username,
    source_channel_id,
    base,
    query_info,
    channel=None,
):
    connection = context.connection
    client = context.client
    producer = context.producer
    status = True
    channel_full = None

    try:
        channel_full = collegram.channels.get_full(
            client,
            channel=channel,
            channel_username=channel_username,
            channel_id=channel_id,
            access_hash=access_hash,
        )
    except (
        ChannelInvalidError,
        ChannelPrivateError,
        UsernameInvalidError,
        ValueError,
    ) as e:
        status = e
        # If multiple keys: for all but ChannelPrivateError, can try with another
        # key
        # ValueError corresponds to deactivated chats
        context.logger.error(
            f"Could not get channel metadata from channel {channel_id}: {repr(e)}"
        )
        flat_channel_d = {
            "id": channel_id,
            "access_hash": 0,
            "source_channel_id": source_channel_id,
            "username": channel_username,
            **base,
            **query_info,
        }
        msg_key = gen_msg_key(flat_channel_d)
        # Update the query info in case of error, so that we don't come back to this
        # same channel on the next iteration.
        if isinstance(e, ChannelPrivateError):
            flat_channel_d["is_private"] = True
            with connection.cursor() as cur:
                upsert_channel(cur, flat_channel_d)
            producer.send(
                "telegram.channel_metadata", key=msg_key, value=flat_channel_d
            )
        else:
            flat_channel_d["is_invalid"] = True
            with connection.cursor() as cur:
                upsert_channel(cur, flat_channel_d)
            producer.send(
                "telegram.channel_metadata", key=msg_key, value=flat_channel_d
            )

    return status, channel_full


def next_channel(context, dt_to):
    connection = context.connection

    try:
        dt_to_str = dt_to.isoformat()
        only_top_priority = "ORDER BY metadata_collection_priority ASC LIMIT 1;"
        where = (
            "WHERE (NOT is_private) AND (NOT is_invalid)"
            f" AND (query_date IS NULL OR query_date < TIMESTAMP '{dt_to_str}')"
        )
        cols = "id, access_hash, username, parent_channel_id, distance_from_core, search_id, keyword_id, keyword, query_date"
        with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"SELECT {cols} FROM telegram.channels {where} {only_top_priority}"
            )
            data = cur.fetchone()

        return data
    except Exception as e:
        context.logger.error(repr(e))
        return False


def single_chan_querier(context, data: dict, lang_priorities: dict):
    producer = context.producer
    client = context.client
    connection = context.connection
    lang_detector = context.lang_detector

    # load the channel data
    source_channel_id = data.get("id")
    parent_channel_id = data.get("parent_channel_id", None)
    access_hash = data.get("access_hash", None)
    channel_username = data.get("username")
    # distance from search core, as in number of hops
    distance_from_core = data.get("distance_from_core", 0)

    # keep track of search
    base = get_base_dict(data)

    # collect full info for channel
    src_query_info = gen_query_info()
    context.logger.info(f"Collecting channel metadata from channel {source_channel_id}")
    status, channel_full = get_full_metadata(
        context,
        source_channel_id,
        access_hash,
        channel_username,
        source_channel_id,
        base,
        src_query_info,
    )
    if status is not True:
        return status
    src_channel_full_d = json.loads(channel_full.to_json())

    chats = [c for c in channel_full.chats if not getattr(c, "deactivated", False)]
    parent_channel = chats[0]
    for c in chats:
        # If channelFull has more than one chat, this condition should be met for a
        # single chat. Else, channelFull corresponds to a public supergroup, and we
        # consider this discussion chat to be its own parent.
        if c.broadcast:
            parent_channel = c

    if parent_channel_id is None:
        parent_channel_id = parent_channel.id
        for c in chats:
            # update destination parent in rels
            with connection.cursor() as cur:
                cur.execute(
                    f"UPDATE {RELS_TABLE}"
                    f" SET destination_parent = {parent_channel.id}"
                    f" WHERE destination = {c.id}",
                )

    context.logger.debug(
        f"Receive relational channel data for channel {source_channel_id}"
    )
    with connection.cursor() as cur:
        # count incoming relations to evaluate priority
        nr_forwarding_channels, nr_linking_channels, nr_recommending_channels = [
            count(
                cur,
                RELS_TABLE,
                {"destination_parent": parent_channel.id, "relation": rel},
            )
            for rel in ("forwarded", "linked", "recommended")
        ]

    # Place parent channel first to attribute its language to its children.
    chats = [parent_channel] + [c for c in chats if c.id != parent_channel.id]

    for chat in chats:
        context.logger.info(f"## Collecting chat metadata for chat {chat.id}")
        # Query only if necessary.
        if chat.id != source_channel_id:
            query_info = gen_query_info()
            status, channel_full = get_full_metadata(
                context,
                chat.id,
                chat.access_hash,
                chat.username,
                source_channel_id,
                base,
                src_query_info,
                channel=chat,
            )
            if status is not True:
                return status
            channel_full_d = json.loads(channel_full.to_json())
        else:
            query_info = src_query_info
            channel_full_d = src_channel_full_d

        query_time = query_info["query_date"]
        channel_full_d.pop("users", None)
        channel_full_d["source_channel_id"] = source_channel_id
        channel_full_d["parent_channel_id"] = parent_channel_id
        # Get count from full_chat and not from chat directly: latter is always
        # null.
        participants_count = channel_full_d["full_chat"]["participants_count"]

        if chat.id == parent_channel.id:
            # language detection on text
            context.logger.debug(f"Detect language from channel {chat.id}")
            lang_code = collegram.text.detect_chan_lang(channel_full_d, lang_detector)
            context.logger.debug(f"language {lang_code} for channel {chat.id}")

        # message counts
        context.logger.debug(
            f"Collecting channel {source_channel_id} chat {chat.id} message counts"
        )
        for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
            c = collegram.messages.get_channel_messages_count(client, chat, f)
            channel_full_d[f"{content_type}_count"] = c

        # (re)evaluate collection priority
        lifespan_seconds = (
            chat.date.replace(tzinfo=None) - query_time.replace(tzinfo=None)
        ).total_seconds()
        context.logger.debug(
            f"Evaluate channel {source_channel_id} chat {chat.id} priority"
        )
        priority = collegram.channels.get_explo_priority(
            lang_code,
            channel_full_d.get("message_count", 1),
            participants_count,
            lifespan_seconds,
            distance_from_core,
            nr_forwarding_channels,
            nr_recommending_channels,
            nr_linking_channels,
            lang_priorities,
            acty_slope=5,
        )

        # write channel info to postgres
        row = {
            "id": chat.id,
            "access_hash": chat.access_hash,
            "username": chat.username,
            "parent_channel_id": parent_channel.id,
            "source_channel_id": source_channel_id,
            "created_at": chat.date.astimezone(datetime.timezone.utc),
            "language_code": lang_code,
            "nr_participants": participants_count,
            "distance_from_core": distance_from_core,
            "message_count": channel_full_d.get("message_count", 1),
            "collection_priority": priority,
            **base,
            **query_info,
        }
        with connection.cursor() as cur:
            upsert_channel(cur, row)

        # recommended
        context.logger.debug(
            f"Collecting channel {source_channel_id} chat {chat.id} recommended"
        )
        recommended_chans = collegram.channels.get_recommended(client, chat)
        # forward recommended to querier
        for recommended in recommended_chans:
            handle_recommended(
                context,
                recommended,
                chat.id,
                source_channel_id,
                parent_channel_id,
                distance_from_core,
                query_info,
                base,
            )

        # send raw channel metadata to iceberg
        # TODO: put recommended channels in there or rely on RELS_TABLE?
        # NOTE: pack all nested fields into single field to avoid schema explosion
        raw_channel_full_d = {
            "id": chat.id,
            "parent_channel_id": parent_channel.id,
            "source_channel_id": source_channel_id,
            "created_at": chat.date.astimezone(datetime.timezone.utc),
            **query_info,
            "raw_channel_metadata": json.dumps(channel_full_d),
        }

        msg_key = gen_msg_key(row)
        producer.send(
            "telegram.raw_channel_metadata", key=msg_key, value=raw_channel_full_d
        )

        # send channel metadata to iceberg
        flat_channel_d = collegram.channels.flatten_dict(
            {**channel_full_d, **query_info}
        )
        producer.send("telegram.channel_metadata", key=msg_key, value=flat_channel_d)

    # done.
    context.logger.info(
        f"Channel {source_channel_id} info collected, {len(chats)} chats"
    )
    return True


def handler(context, event):
    # Triggered by chans_to_query
    nest_asyncio.apply()
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }
    requery_after = datetime.timedelta(days=10)

    data = None
    body = event.body.decode("utf-8")
    if body:
        # load the event data
        data = json.loads(body)

    # self feed
    if data is None or data is False or "id" not in data:
        dt_to = (
            datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
        )
        data = next_channel(context, dt_to)

    if data is not None and data is not False and "id" in data:
        # if data is a dict, it is the channel to query
        try:
            single_chan_querier(context, data, lang_priorities)
        except Exception as e:
            context.logger.error(
                f"Could not get channel metadata from channel {data.get('id')}: {repr(e)}"
            )

    # enqueue the next channel to query
    dt_to = datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
    next = next_channel(context, dt_to)

    # loop if no next available
    while next is None or next is False:
        # wait longer if no data, shorter if error
        delay = WAIT_INTERVAL if next is None else DELAY
        time.sleep(delay)
        dt_to = (
            datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
        )
        next = next_channel(context, dt_to)

    base = get_base_dict(data)
    next = {**next, **base}
    # send channel to be queried
    context.logger.info("Send channel to be queried: {}".format(next.get("id")))

    msg_key = str(dt_to.timestamp()) + str(next.get("id"))
    # msg_json = json.loads(json.dumps(next))
    # send channel to be queried
    context.producer.send("telegram.channels_to_query", key=msg_key, value=next)

    return True
