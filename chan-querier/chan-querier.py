import base64
import datetime
import json
import os
import uuid
from pathlib import Path

import collegram
import nest_asyncio
import psycopg
from kafka import KafkaProducer
from lingua import LanguageDetectorBuilder
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
from telethon.sessions import StringSession
import gc

DB_NAME = os.environ.get("DATABASE_NAME")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_HOST = os.environ.get("DATABASE_HOST")
TELEGRAM_OWNER = os.environ["TELEGRAM_OWNER"]

WAIT_INTERVAL = 60 * 60 * 24  # 24 hours


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


# def _json_default(value):
#     if isinstance(value, datetime):
#         return value.isoformat()
#     else:
#         return repr(value)


def _iceberg_json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return repr(value)


# def iceberg_json_dumps(d: dict):
#     return json.dumps(d, default=_iceberg_json_default).encode("utf-8")


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


def upsert_recommended(cur, source, dest, query_time):
    cur.execute(
        "INSERT INTO telegram.channels_rels (source, destination,relation, first_discovered, last_discovered) "
        f" VALUES({source},{dest},'recommended',%s,%s) "
        " ON CONFLICT(source, destination) "
        f" DO UPDATE SET last_discovered=%s",
        [query_time, query_time, query_time],
    )


def upsert_channel(cur, channel):
    upsert(cur, "telegram.channels", ["id"], channel)


def handler(context, event):

    # Triggered by chans_to_query
    nest_asyncio.apply()
    # Keep, in future if we want to use more than 1 key, this is essential
    key_name = TELEGRAM_OWNER
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }

    producer = context.producer
    client = context.client
    connection = context.connection
    lang_detector = context.lang_detector

    try:
        # messages come from keyword search and peer/link discovery
        # NOTE: we need to avoid looping between peers!
        data = json.loads(event.body.decode("utf-8"))

        channel_id = data.get("id")
        context.logger.debug(f"Receive channel data for channel {channel_id}")

        # read from db to merge with prev values
        with connection.cursor() as cur:
            x = get(
                cur,
                "telegram.channels",
                [
                    "id",
                    "access_hash",
                    "search_id",
                    "distance_from_core",
                    "nr_participants",
                ],
                {"id": channel_id},
            )

            # count incoming relations to evaluate priority
            nr_forwarding_channels = count(
                cur,
                "telegram.channels_rels",
                {
                    "destination": channel_id,
                    "relation": "forwarded",
                },
            )
            nr_recommending_channels = count(
                cur,
                "telegram.channels_rels",
                {
                    "destination": channel_id,
                    "relation": "recommended",
                },
            )
            nr_linking_channels = count(
                cur,
                "telegram.channels_rels",
                {
                    "destination": channel_id,
                    "relation": "linked",
                },
            )

            # distance from search core, as in number of hops
            distance_from_core = (
                min(x.get("distance_from_core", 0), data.get("distance_from_core", 0))
                if x
                else data.get("distance_from_core", 0)
            )

            # number of participants collected from channel info
            nr_participants = (
                max(x.get("nr_participants", 0), data.get("nr_participants", 0))
                if x
                else data.get("nr_participants", 0)
            )

            # LOOP avoidance: if already collected for the same search and fresh, skip

            # collect full info for channel
            query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
            query_info = {
                "query_id": str(uuid.uuid4()),
                "query_date": query_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data_owner": TELEGRAM_OWNER,
            }

            context.logger.info(
                f"Collecting channel metadata from channel {channel_id}"
            )

            channel_full = collegram.channels.get_full(
                client,
                channel_username=data.get("username", None),
                channel_id=data.get("id", None),
                access_hash=data.get("access_hash", None),
            )
            channel_full_d = json.loads(channel_full.to_json())

            context.logger.debug(f"Detect language from channel {channel_id}")

            # language detection on text
            lang_code = collegram.text.detect_chan_lang(channel_full_d, lang_detector)
            context.logger.debug(f"language {lang_code} for channel {channel_id}")

            # Order chats such that one corresponding to channel_full is first, so we don't
            # query it twice
            chats = [c for c in channel_full.chats if c.id == channel_id] + [
                c for c in channel_full.chats if c.id != channel_id
            ]

            # keep track of search
            base = {
                "search_id": data.get("search_id", None),
                "keyword_id": data.get("keyword_id", None),
                "keyword": data.get("keyword", None),
            }

            # record an entry per chat as channel_id,chat_id
            for chat in chats:
                context.logger.info(f"Collecting channel {channel_id} chat {chat.id}")

                chat_d = chat.to_dict()
                channel_chat = {
                    "id": chat_d["id"],
                    "channel_id": channel_id,
                    "access_hash": chat_d["access_hash"],
                    "title": chat_d["title"],
                    "username": chat_d["username"],
                    "nr_participants": (
                        nr_participants
                        if chat_d["participants_count"] is None
                        else chat_d["participants_count"]
                    ),
                    "distance_from_core": distance_from_core,
                    "language_code": (
                        data.get("username") if lang_code is None else lang_code
                    ),
                }

                # message counts
                # DISABLED! this READS all messages!!
                # context.logger.debug(
                #     f"Collecting channel {channel_id} chat {chat.id} message counts"
                # )
                # counts = {
                #     k: v
                #     for k, v in [
                #         [
                #             f"{content_type}_count",
                #             collegram.messages.get_channel_messages_count(
                #                 client, chat, f
                #             ),
                #         ]
                #         for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items()
                #     ]
                # }

                row = base.copy() | query_info.copy() | channel_chat  # | counts

                # recommended
                context.logger.debug(
                    f"Collecting channel {channel_id} chat {chat.id} recommended"
                )
                recommended_chans = collegram.channels.get_recommended(client, chat)
                row["nr_recommended"] = len(recommended_chans)

                # (re)evaluate collection priority
                lifespan_seconds = (
                    chat.date.replace(tzinfo=None) - query_time.replace(tzinfo=None)
                ).total_seconds()

                context.logger.debug(
                    f"Evaluate channel {channel_id} chat {chat.id} priority"
                )
                priority = collegram.channels.get_explo_priority(
                    channel_chat["language_code"],
                    1,  # counts.get("message_count", 0),
                    chat.participants_count,
                    lifespan_seconds,
                    distance_from_core,
                    nr_forwarding_channels,
                    nr_recommending_channels,
                    nr_linking_channels,
                    lang_priorities,
                    acty_slope=5,
                )

                row["collection_priority"] = priority

                context.logger.debug(
                    f"row: {json.dumps(row, default=_iceberg_json_default)}"
                )

                # write channel info to postgres
                upsert_channel(cur, row)

                # forward recommended to querier
                for recommended in recommended_chans:
                    context.logger.debug(
                        f"Process channel {channel_id} chat {chat.id} recommended {recommended.id}"
                    )

                    # check if exists and fresh to avoid loops between peers
                    z = get(
                        cur,
                        "telegram.channels_rels",
                        ["source", "destination", "last_discovered"],
                        {
                            "source": channel_id,
                            "destination": recommended.id,
                            "relation": "recommended",
                        },
                    )

                    if (
                        not z
                        or (query_time - z["last_discovered"]).total_seconds()
                        > WAIT_INTERVAL
                    ):
                        context.logger.debug(
                            f"Record channel {channel_id} chat {chat.id} recommended {recommended.id}"
                        )

                        # split recommended into table to obtain an adjacency matrix
                        upsert_recommended(
                            cur,
                            channel_id,
                            recommended.id,
                            query_time,
                        )
                        #  send to querier
                        message = base.copy() | {
                            "id": recommended.id,
                            "access_hash": recommended.access_hash,
                            "title": recommended.title,
                            "username": recommended.username,
                            "nr_participants": recommended.participants_count,
                            "distance_from_core": distance_from_core + 1,
                        }

                        # NOTE: this breaks on loops: if we get the same channel from the same search
                        # via another source the messages will collide
                        # TODO handle, for now we let it break to avoid duplicates
                        msg_key = message["search_id"] + "|" + str(message["id"])

                        producer.send(
                            "telegram.channels_to_query",
                            key=msg_key,
                            value=json.loads(json.dumps(message)),
                        )

            # done.
            context.logger.info(
                f"Channel {channel_id} info collected, {len(chats)} chats"
            )

            return context.Response(
                body=f"Channel {channel_id} info collected",
                headers={},
                content_type="text/plain",
                status_code=200,
            )

    except (
        ChannelInvalidError,
        ChannelPrivateError,
        UsernameInvalidError,
        ValueError,
    ) as e:
        # TODO What then?
        # For all but ChannelPrivateError, can try with another key (TODO: add to
        # list of new channels?).
        context.logger.warning(
            f"Could not get channel metadata from channel {channel_id}"
        )

        raise e

    # force garbage collection
    gc.collect()
