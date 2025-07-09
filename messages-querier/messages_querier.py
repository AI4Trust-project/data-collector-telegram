import datetime
import json
import os
import time
import uuid
from pathlib import Path
from typing import Optional

import collegram
import fsspec
import nest_asyncio
import polars as pl
import psycopg
import psycopg.rows
from kafka import KafkaProducer
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
from telethon.sessions import StringSession
from telethon.types import InputPeerChannel, MessageService

DB_NAME = os.environ.get("DATABASE_NAME")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_HOST = os.environ.get("DATABASE_HOST")
TELEGRAM_OWNER = os.environ["TELEGRAM_OWNER"]

RELS_TABLE = "telegram.channels_rels"
WAIT_INTERVAL = 60 * 60  # 1 hour
DELAY = 10

EMOJIS = ['🔥','❤','🤬','🤡','👎','👍','😱','😢','😁','😐','💯','🤮','👏','🤔','🙏','🤨','💔','🫡','❤‍🔥','😭','👀','🤝','✍','⚡','🙈','🤷‍♂','🤯','🕊','🤩','💊','💩','🏆','🥴','👌','🦄','🎅','🎄','🐳','🌭','☃','🤪','👾','🍌','👻','😎','💋','👨‍💻','💅','🌚','🤗','🖕','😍','🤣','🥱','😨','🗿','🤷‍♀','😡','🥰','🙊','🍓','🙉','😴','😈','🍾','🎉','🆒','💘','🤷','😇','🤓','😘','🎃']
EMOJIS_NAMES = {'🔥': 'FIRE', '❤': 'HEAVY_BLACK_HEART', '🤬': 'SERIOUS_FACE_WITH_SYMBOLS_COVERING_MOUTH', '🤡': 'CLOWN_FACE', '👎': 'THUMBS_DOWN_SIGN', '👍': 'THUMBS_UP_SIGN', '😱': 'FACE_SCREAMING_IN_FEAR', '😢': 'CRYING_FACE', '😁': 'GRINNING_FACE_WITH_SMILING_EYES', '😐': 'NEUTRAL_FACE', '💯': 'HUNDRED_POINTS_SYMBOL', '🤮': 'FACE_WITH_OPEN_MOUTH_VOMITING', '👏': 'CLAPPING_HANDS_SIGN', '🤔': 'THINKING_FACE', '🙏': 'PERSON_WITH_FOLDED_HANDS', '🤨': 'FACE_WITH_ONE_EYEBROW_RAISED', '💔': 'BROKEN_HEART', '🫡': 'SALUTING_FACE', '❤\u200d🔥': 'HEAVY_BLACK_HEART', '😭': 'LOUDLY_CRYING_FACE', '👀': 'EYES', '🤝': 'HANDSHAKE', '✍': 'WRITING_HAND', '⚡': 'HIGH_VOLTAGE_SIGN', '🙈': 'SEE-NO-EVIL_MONKEY', '🤷\u200d♂': 'SHRUG', '🤯': 'SHOCKED_FACE_WITH_EXPLODING_HEAD', '🕊': 'DOVE_OF_PEACE', '🤩': 'GRINNING_FACE_WITH_STAR_EYES', '💊': 'PILL', '💩': 'PILE_OF_POO', '🏆': 'TROPHY', '🥴': 'FACE_WITH_UNEVEN_EYES_AND_WAVY_MOUTH', '👌': 'OK_HAND_SIGN', '🦄': 'UNICORN_FACE', '🎅': 'FATHER_CHRISTMAS', '🎄': 'CHRISTMAS_TREE', '🐳': 'SPOUTING_WHALE', '🌭': 'HOT_DOG', '☃': 'SNOWMAN', '🤪': 'GRINNING_FACE_WITH_ONE_LARGE_AND_ONE_SMALL_EYE', '👾': 'ALIEN_MONSTER', '🍌': 'BANANA', '👻': 'GHOST', '😎': 'SMILING_FACE_WITH_SUNGLASSES', '💋': 'KISS_MARK', '👨\u200d💻': 'MAN', '💅': 'NAIL_POLISH', '🌚': 'NEW_MOON_WITH_FACE', '🤗': 'HUGGING_FACE', '🖕': 'REVERSED_HAND_WITH_MIDDLE_FINGER_EXTENDED', '😍': 'SMILING_FACE_WITH_HEART-SHAPED_EYES', '🤣': 'ROLLING_ON_THE_FLOOR_LAUGHING', '🥱': 'YAWNING_FACE', '😨': 'FEARFUL_FACE', '🗿': 'MOYAI', '🤷\u200d♀': 'SHRUG', '😡': 'POUTING_FACE', '🥰': 'SMILING_FACE_WITH_SMILING_EYES_AND_THREE_HEARTS', '🙊': 'SPEAK-NO-EVIL_MONKEY', '🍓': 'STRAWBERRY', '🙉': 'HEAR-NO-EVIL_MONKEY', '😴': 'SLEEPING_FACE', '😈': 'SMILING_FACE_WITH_HORNS', '🍾': 'BOTTLE_WITH_POPPING_CORK', '🎉': 'PARTY_POPPER', '🆒': 'SQUARED_COOL', '💘': 'HEART_WITH_ARROW', '🤷': 'SHRUG', '😇': 'SMILING_FACE_WITH_HALO', '🤓': 'NERD_FACE', '😘': 'FACE_THROWING_A_KISS', '🎃': 'JACK-O-LANTERN'}

def emoji_to_name(e):
    if e and e[:1] in EMOJIS_NAMES:
        return EMOJIS_NAMES[e]
    return False


def process_reactions(reactions):
    if not reactions:
        return None,None

    # we loop over definition to keep full list consistent
    processed_reactions = {EMOJIS_NAMES.get(k): 0 for k in EMOJIS_NAMES}
    other_reactions = 0

    # collect reactions
    for reaction, count in reactions.items():
        name = emoji_to_name(reaction)
        if name and name in processed_reactions:
            processed_reactions[name] = count if count else 0
        else:
            #increment other reactions
            other_reactions += count if count else 0
    return processed_reactions,other_reactions

async def init_context(context):
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret = os.environ["MINIO_SECRET_KEY"]
    minio_home = os.environ["MINIO_HOME"]
    storage_options = {
        "endpoint_url": f"https://{minio_home}",
        "key": access_key,
        "secret": secret,
    }
    fs = fsspec.filesystem("s3", **storage_options)
    setattr(context, "fs", fs)

    # Connect to an existing database
    connection = psycopg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        dbname=DB_NAME,
    )
    connection.autocommit = True
    setattr(context, "connection", connection)

    # prefix = os.environ["TELEGRAM_OWNER"].upper()
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


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    else:
        return repr(value)


def _iceberg_json_default(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, set):
        return list(value)
    else:
        return repr(value)


def iceberg_json_dumps(d: dict):
    return json.dumps(d, default=_iceberg_json_default).encode("utf-8")


def gen_query_info(query_time=None):
    if query_time is None:
        query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
    return {
        "query_id": str(uuid.uuid4()),
        "query_date": query_time,
        "data_owner": TELEGRAM_OWNER,
    }


def gen_msg_key(row: dict):
    return "+".join(
        str(p)
        for p in [
            row.get("search_id"),
            row["id"],
            row["query_id"],
        ]
    )


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


def upsert_channel(cur, channel):
    upsert(cur, "telegram.channels", ["id"], channel)


def upsert_relation(
    cur,
    source,
    source_parent,
    dest,
    dest_parent,
    dest_username,
    relation,
    nr_messages,
    first_message_date,
    last_message_date,
):
    cols = "(source, source_parent, destination, destination_parent, destination_username, relation, nr_messages, first_discovered, last_discovered)"
    base_query = (
        (
            f"INSERT INTO {RELS_TABLE} {cols} "
            f" VALUES({source}, {source_parent}, {dest}, {dest_parent}, '{dest_username}', '{relation}', {nr_messages}, %s, %s) "
        )
        .replace("'None'", "NULL")
        .replace("None", "NULL")
    )

    if dest is not None:
        where = (
            f"WHERE relation='{relation}' AND source={source} AND destination={dest}"
        )
    elif dest_username is not None:
        where = f"WHERE relation='{relation}' AND source={source} AND destination_username='{dest_username}'"
    else:
        raise ValueError("one of destination's ID or username must be set")

    cur.execute(f"SELECT {cols} FROM {RELS_TABLE} {where}")
    rel_data = cur.fetchone()
    if rel_data is None:
        cur.execute(base_query, [first_message_date, last_message_date])
    else:
        cur.execute(
            f"UPDATE {RELS_TABLE} SET last_discovered = %s, nr_messages = nr_messages + {nr_messages} {where}",
            [last_message_date],
        )


def get_input_chan(
    client,
    channel_username: Optional[str] = None,
    channel_id: Optional[int] = None,
    access_hash: Optional[int] = None,
):
    try:
        fwd_input_peer_channel = collegram.channels.get_input_peer(
            client, channel_username, channel_id, access_hash
        )
        return fwd_input_peer_channel
    except ChannelPrivateError:
        # These channels are valid and have been seen for sure,
        # might be private though. TODO: keep track of private channels!
        return
    except (ChannelInvalidError, UsernameInvalidError, ValueError):
        # This should happen extremely rarely, still haven't figured
        # out conditions under which it does.
        return


def get_new_link_stats(prev_stats, update_stats):
    if prev_stats is None:
        new_stats = update_stats
    else:
        new_stats = {
            "nr_messages": prev_stats.get("nr_messages", 0)
            + update_stats["nr_messages"],
            "first_message_date": prev_stats.get(
                "first_message_date", update_stats["first_message_date"]
            ),
            "last_message_date": update_stats["last_message_date"],
        }
    return new_stats


def reassign_prio(chan_data, pred_dist_from_core, lang_priorities, connection):
    with connection.cursor() as cur:
        # count incoming relations to evaluate priority
        nr_forwarding_channels, nr_linking_channels, nr_recommending_channels = [
            count(
                cur,
                RELS_TABLE,
                {
                    "destination_parent": chan_data["parent_channel_id"],
                    "relation": rel,
                },
            )
            for rel in ("forwarded", "linked", "recommended")
        ]

    new_dist_from_core = min(pred_dist_from_core + 1, chan_data["distance_from_core"])
    lifespan_seconds = (
        chan_data["query_date"].replace(tzinfo=None)
        - chan_data["created_at"].replace(tzinfo=None)
    ).total_seconds()
    priority = collegram.channels.get_explo_priority(
        chan_data["language_code"],
        chan_data["message_count"],
        chan_data["nr_participants"],
        lifespan_seconds,
        new_dist_from_core,
        nr_forwarding_channels,
        nr_recommending_channels,
        nr_linking_channels,
        lang_priorities,
        acty_slope=5,
    )
    central_priority = collegram.channels.get_centrality_score(
        new_dist_from_core,
        nr_forwarding_channels,
        nr_recommending_channels,
        nr_linking_channels,
    )
    update_d = {
        "id": chan_data["id"],
        "distance_from_core": new_dist_from_core,
        "collection_priority": priority,
        "metadata_collection_priority": central_priority,
    }
    update_d["collection_priority"] = priority
    collegram.utils.update_postgres(connection, "telegram.channels", update_d, "id")


def handle_linked(
    channel_id,
    parent_channel_id,
    linked_username,
    link_stats,
    client,
    connection,
    pred_dist_from_core,
    lang_priorities,
):
    # Insert channel if never found before
    base_query = (
        "SELECT"
        " id,"
        " parent_channel_id,"
        " created_at,"
        " query_date,"
        " language_code,"
        " nr_participants,"
        " message_count,"
        " distance_from_core"
        " FROM telegram.channels"
    )
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(base_query + f" WHERE username = '{linked_username}'")
        linked_data = cur.fetchone()

    if linked_data is None:
        dist_from_core = pred_dist_from_core + 1
        priority = collegram.channels.get_centrality_score(dist_from_core, 0, 0, 1)
        linked_data = {
            "username": linked_username,
            "data_owner": os.environ["TELEGRAM_OWNER"],
            "distance_from_core": dist_from_core,
            "metadata_collection_priority": priority,
        }
        collegram.utils.insert_into_postgres(
            connection, "telegram.channels", linked_data
        )

    # Upsert relation. If the parent ID is unknown (so didn't go through
    # `chan-querier`), consider channel as its own parent.
    linked_parent_channel_id = linked_data.get("parent_channel_id") or linked_data.get(
        "id"
    )
    with connection.cursor() as cur:
        upsert_relation(
            cur,
            channel_id,
            parent_channel_id,
            linked_data.get("id"),
            linked_parent_channel_id,
            linked_username,
            "linked",
            **link_stats,
        )

    # If channel has already been queried by `chan-querier`, then recompute
    # priority.
    if linked_data.get("query_date") is not None:
        reassign_prio(linked_data, pred_dist_from_core, lang_priorities, connection)


def handle_forwarded(
    channel_id,
    parent_channel_id,
    fwd_id,
    fwd_stats,
    client,
    connection,
    pred_dist_from_core,
    lang_priorities,
):
    # Insert channel if never found before
    base_query = (
        "SELECT"
        " id,"
        " parent_channel_id,"
        " created_at,"
        " query_date,"
        " language_code,"
        " nr_participants,"
        " message_count,"
        " distance_from_core"
        " FROM telegram.channels"
    )
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(base_query + f" WHERE id = {fwd_id}")
        fwd_data = cur.fetchone()

    if fwd_data is None:
        input_peer_channel = get_input_chan(client, channel_id=fwd_id)
        if isinstance(input_peer_channel, InputPeerChannel):
            dist_from_core = pred_dist_from_core + 1
            priority = collegram.channels.get_centrality_score(dist_from_core, 1, 0, 0)
            fwd_data = {
                "id": input_peer_channel.channel_id,
                "parent_channel_id": input_peer_channel.channel_id,
                "access_hash": input_peer_channel.access_hash,
                "data_owner": os.environ["TELEGRAM_OWNER"],
                "distance_from_core": dist_from_core,
                "metadata_collection_priority": priority,
            }
            collegram.utils.insert_into_postgres(
                connection, "telegram.channels", fwd_data
            )

    if fwd_data is not None:
        # Upsert relation. If the parent ID is unknown (so didn't go through
        # `chan-querier`), consider channel as its own parent.
        fwd_parent_channel_id = fwd_data["parent_channel_id"] or fwd_data["id"]
        with connection.cursor() as cur:
            upsert_relation(
                cur,
                channel_id,
                parent_channel_id,
                fwd_data["id"],
                fwd_parent_channel_id,
                None,
                "forwarded",
                **fwd_stats,
            )

        # If channel has already been queried by `chan-querier`, then recompute
        # priority.
        if fwd_data.get("query_date") is not None:
            reassign_prio(fwd_data, pred_dist_from_core, lang_priorities, connection)


def gen_message_msg_key(row: dict):
    return "+".join(
        str(p)
        for p in [
            row["channel_id"],
            row["query_id"],
            row["id"],
        ]
    )


async def collect_messages(
    client: TelegramClient,
    channel,
    dt_from: datetime.datetime,
    dt_to: datetime.datetime,
    forwards_stats: dict[int, dict],
    linked_chans_stats: dict[str, dict],
    anon_func,
    media_save_path: Path,
    fs,
    producer,
    query_info,
    offset_id=0,
):
    last_id = offset_id
    # Pass `fs` to following call to write embedded web pages as json artifacts
    async for m in collegram.messages.yield_channel_messages(
        client,
        channel,
        dt_from,
        dt_to,
        forwards_stats,
        linked_chans_stats,
        anon_func,
        media_save_path,
        offset_id=offset_id,
        fs=fs,
    ):
        # m_dict = {**m.to_dict(), **query_info}
        # raw message as nested json
        m_dict = {"id": m.id, "date": m.date, **query_info, "raw_message": json.dumps(m.to_dict(), default=_iceberg_json_default)}
        msg_key = gen_message_msg_key(m_dict)
        # MessageService have so many potential structures that putting them together
        # with normal messages in a table does not make sense.
        if isinstance(m, MessageService):
            producer.send("telegram.raw_service_messages", key=msg_key, value=m_dict)
        else:
            producer.send("telegram.raw_messages", key=msg_key, value=m_dict)
            # flat_dict = collegram.messages.to_flat_dict(m)
            flat_dict = {**collegram.messages.to_flat_dict(m), **query_info}

            # replace reactions with fixed structure
            reactions, other_reactions = process_reactions(flat_dict["reactions"]) if "reactions" in flat_dict else (None, None)
            flat_dict["reactions"] = reactions
            flat_dict["other_reactions"] = other_reactions

            producer.send("telegram.messages", key=msg_key, value=flat_dict)
        last_id = m.id
    return last_id


def next_channel(context, dt_to):
    connection = context.connection

    try:
        dt_to_str = dt_to.isoformat()
        only_top_priority = "ORDER BY collection_priority ASC LIMIT 1"
        can_query = (
            "(NOT is_private) AND (NOT is_invalid) AND collection_priority IS NOT NULL"
        )
        cols = "id, access_hash, username, parent_channel_id, messages_last_queried_at, last_queried_message_id, distance_from_core"
        with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # First look for already-queried channel for which we need new messages
            cur.execute(
                f"SELECT {cols} FROM telegram.channels"
                f" WHERE {can_query}"
                f" AND messages_last_queried_at < TIMESTAMP '{dt_to_str}'"
                f" {only_top_priority}"
            )
            data = cur.fetchone()

            # If there is none, query a new one.
            if data is None:
                cur.execute(
                    f"SELECT {cols} FROM telegram.channels"
                    f" WHERE {can_query}"
                    f" AND messages_last_queried_at IS NULL"
                    f" {only_top_priority}"
                )
                data = cur.fetchone()

        return data

    except Exception as e:
        context.logger.error(repr(e))
        return False


def single_chan_messages_querier(
    context,
    data: dict,
    start_at: datetime.datetime,
    stop_at: datetime.datetime,
    lang_priorities: dict,
):
    producer = context.producer
    client = context.client
    connection = context.connection
    fs = context.fs

    try:
        # load the channel data
        channel_id = data.get("id")
        parent_channel_id = data.get("parent_channel_id", None)
        access_hash = data.get("access_hash", None)
        channel_username = data.get("username")
        messages_last_queried_at = data.get("messages_last_queried_at")
        message_offset_id = data.get("last_queried_message_id", 0)
        # distance from search core, as in number of hops
        distance_from_core = data.get("distance_from_core", 0)

        data_path = Path("/telegram/")
        paths = collegram.paths.ProjectPaths(data=data_path)
        media_save_path = paths.raw_data / "media"

        dt_from = messages_last_queried_at or start_at
        if isinstance(dt_from, str):
            dt_from = datetime.datetime.fromisoformat(dt_from)
        dt_from = dt_from.astimezone(datetime.timezone.utc)

        query_info = gen_query_info()
        try:
            input_chat = collegram.channels.get_input_peer(
                client, channel_username, channel_id, access_hash
            )
        except (
            ChannelInvalidError,
            ChannelPrivateError,
            UsernameInvalidError,
            ValueError,
        ) as e:
            # If multiple keys: for all but ChannelPrivateError, can try with another
            # key
            # ValueError corresponds to deactivated chats
            context.logger.error(
                f"Could not get channel metadata from channel {channel_id}"
            )
            flat_channel_d = {
                "id": channel_id,
                "username": channel_username,
                "is_private": True,
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
            return e

        context.logger.info(
            f"# Collecting messages from {channel_username} username, with ID {channel_id}"
        )
        anon_func = lambda x: x

        # Collect by chunks of maximum a month to limit effects of a crash on the
        # collection.
        dt_bin_edges = pl.datetime_range(
            dt_from, stop_at, interval="1mo", eager=True, time_zone="UTC"
        )
        if len(dt_bin_edges) < 2:
            dt_bin_edges = [dt_from, stop_at]

        forwarded_chans_stats = {}
        linked_chans_stats = {}

        for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
            chunk_fwds_stats = {}
            chunk_linked_chans_stats = {}

            query_info = gen_query_info()
            query_info["channel_id"] = input_chat.channel_id
            query_info["message_offset_id"] = message_offset_id

            context.logger.info(
                f"## Collecting messages from {dt_from.date()} to {dt_to.date()}"
            )
            last_queried_message_id = client.loop.run_until_complete(
                collect_messages(
                    client,
                    input_chat,
                    dt_from,
                    dt_to,
                    chunk_fwds_stats,
                    chunk_linked_chans_stats,
                    anon_func,
                    media_save_path,
                    fs,
                    producer,
                    query_info,
                    offset_id=message_offset_id,
                )
            )

            context.logger.info("## Updating channel query info in postgres")
            update_d = {
                "id": channel_id,
                "last_queried_message_id": last_queried_message_id,
                "messages_last_queried_at": query_info["query_date"],
            }
            collegram.utils.update_postgres(
                connection, "telegram.channels", update_d, "id"
            )

            context.logger.info(f"## Handling {len(chunk_fwds_stats)} forwarded chans")
            for fwd_id, fwd_stats in chunk_fwds_stats.items():
                context.logger.debug(f"### Handling forwarded {fwd_id}")
                prev_stats = forwarded_chans_stats.get(fwd_id)
                end_chunk_stats = get_new_link_stats(prev_stats, fwd_stats)
                handle_forwarded(
                    channel_id,
                    parent_channel_id,
                    fwd_id,
                    end_chunk_stats,
                    client,
                    connection,
                    distance_from_core,
                    lang_priorities,
                )
                forwarded_chans_stats[fwd_id] = end_chunk_stats

            context.logger.info(
                f"## Handling {len(chunk_linked_chans_stats)} linked chans"
            )
            for link_un, link_stats in chunk_linked_chans_stats.items():
                context.logger.debug(f"### Handling linked {link_un}")
                prev_stats = linked_chans_stats.get(link_un)
                end_chunk_stats = get_new_link_stats(prev_stats, link_stats)
                handle_linked(
                    channel_id,
                    parent_channel_id,
                    link_un,
                    end_chunk_stats,
                    client,
                    connection,
                    distance_from_core,
                    lang_priorities,
                )
                linked_chans_stats[link_un] = end_chunk_stats

        # done.
        context.logger.info(f"# Channel {channel_id} messages collected")
        return True

    except Exception as e:
        context.logger.error(
            f"Could not get messages from channel {channel_id}: {repr(e)}"
        )
        return e


def handler(context, event):
    nest_asyncio.apply()
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }
    requery_after = datetime.timedelta(days=1)

    data = None
    body = event.body.decode("utf-8")
    if body:
        # load the event data
        data = json.loads(body)

    # self feed
    if not isinstance(data, dict) or "id" not in data:
        dt_to = (
            datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
        )
        data = next_channel(context, dt_to)

    if isinstance(data, dict) and "id" in data:
        # if data is a dict, it is the channel to query
        try:
            start_at = datetime.datetime(2025, 1, 1).astimezone(datetime.timezone.utc)
            # Stop two days before now in order to get more or less final reaction + view counts
            stop_at = datetime.datetime.now().astimezone(
                datetime.timezone.utc
            ) - datetime.timedelta(days=2)

            single_chan_messages_querier(
                context, data, start_at, stop_at, lang_priorities
            )
        except Exception as e:
            context.logger.error(
                f"Could not get messages from channel {data.get('id')}: {repr(e)}"
            )

    # min wait to stagger requests
    time.sleep(0.5)
    # enqueue the next channel to query
    dt_to = datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
    next_data = next_channel(context, dt_to)

    # loop if no next available
    while not isinstance(next_data, dict):
        # wait longer if no data, shorter if error
        delay = WAIT_INTERVAL if next_data is None else DELAY
        time.sleep(delay)
        dt_to = (
            datetime.datetime.now().astimezone(datetime.timezone.utc) - requery_after
        )
        next_data = next_channel(context, dt_to)

    # send channel to be queried
    context.logger.info("Send channel to be queried: {}".format(next_data.get("id")))

    msg_key = str(dt_to.timestamp()) + str(next_data.get("id"))
    # send channel to be queried
    context.producer.send("telegram.channels_to_scrape", key=msg_key, value=next_data)

    return True
