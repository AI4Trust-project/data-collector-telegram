import datetime
import json
import os
import time
import uuid
from datetime import timezone

import nest_asyncio
import psycopg
from kafka import KafkaProducer
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import SearchRequest

DB_NAME = os.environ.get("DATABASE_NAME")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_HOST = os.environ.get("DATABASE_HOST")
TELEGRAM_OWNER = os.environ["TELEGRAM_OWNER"]

DELAY = 15
LANGUAGE_CODES = {
    "english": "EN",
    "french": "FR",
    "spanish": "ES",
    "german": "DE",
    "greek": "EL",
    "italian": "IT",
    "polish": "PL",
    "romanian": "RO",
}


async def init_context(context):
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
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BROKER"),
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def get_keywords(conn, kid):
    """Get keywords from database"""
    cur = None
    data = []

    try:
        cur = conn.cursor()

        query = "SELECT keyword_id, keyword, lang, data_owner, topic FROM telegram.search_keywords ORDER BY keyword_id"
        if kid and kid > 0:
            query = f"SELECT keyword_id, keyword, lang, data_owner, topic FROM telegram.search_keywords WHERE keyword_id='{kid}'"

        cur.execute(query)
        row = cur.fetchall()

        if row:
            for keyword_id, keyword, lang, data_owner, topic in row:
                config = {
                    "keyword_id": keyword_id,
                    "keyword": keyword,
                    "lang": lang,
                    "data_owner": data_owner,
                    "topic": topic,
                }
                data.append(config)
    except Exception as e:
        print("ERROR FIND KEYWORDS:")
        print(e)
    finally:
        cur.close()

    return data


def insert_if_missing(cur, table, pk_fields, kwargs):
    # check to see if it already exists
    where = " AND ".join("%s=%%s" % pkf for pkf in pk_fields)
    where_args = [kwargs[pkf] for pkf in pk_fields]
    cur.execute("SELECT COUNT(*) FROM %s WHERE %s LIMIT 1" % (table, where), where_args)
    fields = [f for f in kwargs.keys()]
    if cur.fetchone()[0] > 0:
        return False
    else:
        field_placeholders = ["%s"] * len(fields)
        fmt_args = (table, ",".join(fields), ",".join(field_placeholders))
        insert_args = [kwargs[f] for f in fields]
        cur.execute("INSERT INTO %s (%s) VALUES (%s)" % fmt_args, insert_args)
        return True


def handler(context, event):
    # add nest asyncio for waiting calls
    nest_asyncio.apply()

    producer = context.producer
    client = context.client

    kid = None

    body = event.body.decode("utf-8")
    if body:
        parameters = json.loads(body)
        if "keyword_id" in parameters:
            kid = int(parameters["keyword_id"])

    with psycopg.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, dbname=DB_NAME
    ) as connection:
        keywords_data = get_keywords(connection, kid)

    context.logger.info(" Started keyword search")
    for keyword in keywords_data:
        context.logger.info(f"Search for keyword {keyword}")
        try:
            date = datetime.datetime.now().astimezone(timezone.utc)
            query_uuid = str(uuid.uuid4())
            kw = keyword.get("keyword", None)
            lang_code = LANGUAGE_CODES[keyword["lang"].lower()]
            channels = client.loop.run_until_complete(
                client(SearchRequest(q=kw, limit=100))
            ).chats

            # log search
            search_row = {
                "id": query_uuid,
                "data_owner": keyword.get("data_owner", TELEGRAM_OWNER),
                "created_at": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "keyword_id": keyword.get("keyword_id", None),
                "keyword": kw,
                "topic": keyword.get("topic", ""),
                "language_code": lang_code,
                "results": len(channels),
            }

            producer.send(
                "telegram.search_parameters",
                key=search_row["id"],
                value=json.loads(json.dumps(search_row)),
            )

            # track channels discovered from search
            base = {
                "language_code": lang_code,
                "search_id": query_uuid,
                "keyword_id": keyword.get("keyword_id", None),
                "keyword": keyword.get("keyword", None),
                "distance_from_core": 0,
            }

            with psycopg.connect(
                user=DB_USER, password=DB_PASSWORD, host=DB_HOST, dbname=DB_NAME
            ) as connection:
                connection.autocommit = True
                with connection.cursor() as cur:
                    for channel in channels:
                        chan_row = {
                            "id": channel.id,
                            "access_hash": channel.access_hash,
                            "username": channel.username,
                            **base,
                        }
                        insert_if_missing(cur, "telegram.channels", ["id"], chan_row)

            # wait to stagger requests
            time.sleep(DELAY)

        except Exception as e:
            context.logger.error(f"Error with search for keyword: {e}")
            continue

    return context.Response(
        body=f"Run search for channels",
        headers={},
        content_type="text/plain",
        status_code=200,
    )
