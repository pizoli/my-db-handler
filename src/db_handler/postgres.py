# Copyright 2026 Zoltan Pinter
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    Univerzális PostgreSQL kezelő osztály Connection Pooling támogatással.
    Segít abban, hogy a notifier, register és parser modulok hatékonyan
    osztozzanak az adatbázis erőforrásain.
    """
    _pool: Optional[ThreadedConnectionPool] = None

    def __init__(self, host, database, user, password, port=5432, min_conn=1, max_conn=10):
        """
        Inicializálja a kapcsolatgyűjteményt (Pool), ha még nem létezik.
        """
        if PostgresClient._pool is None:
            try:
                PostgresClient._pool = ThreadedConnectionPool(
                    min_conn,
                    max_conn,
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                logger.info("PostgreSQL Connection Pool sikeresen létrehozva.")
            except Exception as e:
                logger.error(f"Hiba a pool létrehozásakor: {e}")
                raise

    @contextmanager
    def get_cursor(self):
        """
        Context manager, amely automatikusan kéri és adja vissza a kapcsolatot a pool-ba.
        Használat: with client.get_cursor() as cur: ...
        """
        current_pool = PostgresClient._pool

        if current_pool is None:
            raise RuntimeError(
                "A PostgreSQL Connection Pool nincs inicializálva! "
                "Példányosítsd a PostgresClient osztályt a használat előtt."
            )

        conn = current_pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    yield cur
        except Exception as e:
            logger.error(f"Adatbázis hiba: {e}")
            raise e
        finally:
            current_pool.putconn(conn)

    def fetch_all(self, query, params=None):
        """Lekérdezés futtatása és minden találat visszaadása (SELECT)."""
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute(self, query, params=None):
        """Módosító művelet (INSERT, UPDATE, DELETE) futtatása."""
        with self.get_cursor() as cur:
            cur.execute(query, params)
