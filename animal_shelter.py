from pymongo import MongoClient, errors
import logging
import time

logger = logging.getLogger(__name__)


class AnimalShelter:
    """
    MongoDB data access for the dashboard.
    Provides CRUD operations with logging, basic validation, and resilient reads.
    """

    # Minimal set of fields expected by downstream code/visuals
    REQUIRED_FIELDS = ("Animal ID", "Breed")

    def __init__(self, user, password, host, port, db, col):
        """
        Create a client and bind to the collection.
        Uses a short serverSelectionTimeout so the UI can fall back quickly.
        """
        if user and password:
            uri = f"mongodb://{user}:{password}@{host}:{port}/{db}"
        else:
            uri = f"mongodb://{host}:{port}/{db}"

        logger.info("Connecting to MongoDB: %s/%s (timeout=5s)", host, db)
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.database = self.client[db]
        self.collection = self.database[col]

    # ---------- Helper: schema validation ----------
    def _is_valid_doc(self, doc: dict) -> bool:
        """Return True if the document contains the required fields."""
        try:
            return all(k in doc for k in self.REQUIRED_FIELDS)
        except Exception:
            return False

    # ---------- CRUD ----------
    def create(self, data):
        """
        Insert one document.
        Returns: {"inserted_id": <str>, "acknowledged": <bool>}
        """
        if not data or not isinstance(data, dict):
            raise ValueError("create() requires a non-empty dict")
        try:
            result = self.collection.insert_one(data)
            logger.info(
                "create: inserted_id=%s acknowledged=%s",
                result.inserted_id,
                result.acknowledged,
            )
            return {
                "inserted_id": str(result.inserted_id),
                "acknowledged": result.acknowledged,
            }
        except errors.PyMongoError as e:
            logger.error("create failed: %s", e)
            raise

    def read(self, query=None, projection=None, limit=None, retries: int = 3):
        """
        Read documents with optional query, projection, and limit.
        Adds retry with exponential backoff and basic schema validation.
        Returns: list[dict]
        """
        query = query or {}
        attempt, delay = 0, 1.0
        last_exc = None

        while attempt < retries:
            try:
                t0 = time.perf_counter()
                cursor = self.collection.find(query, projection)
                if limit is not None:
                    cursor = cursor.limit(limit)
                docs = list(cursor)
                ms = (time.perf_counter() - t0) * 1000.0

                # Basic validation: keep only docs with required fields
                valid_docs = [d for d in docs if self._is_valid_doc(d)]
                if len(valid_docs) < len(docs):
                    logger.warning(
                        "read: %d/%d docs dropped due to missing required fields %s",
                        len(docs) - len(valid_docs),
                        len(docs),
                        self.REQUIRED_FIELDS,
                    )

                logger.info(
                    "read: query=%s projection=%s limit=%s -> %d valid docs (%.1f ms)",
                    query if query else "{}",
                    list(projection.keys()) if isinstance(projection, dict) else projection,
                    limit,
                    len(valid_docs),
                    ms,
                )
                return valid_docs
            except errors.PyMongoError as e:
                last_exc = e
                attempt += 1
                logger.warning(
                    "read failed (attempt %d/%d): %s; retrying in %.1fs",
                    attempt, retries, e, delay
                )
                time.sleep(delay)
                delay *= 2  # exponential backoff

        # Exhausted retries: re-raise so caller can fall back (e.g., to CSV)
        logger.error("read failed after %d attempts; raising last exception", retries)
        raise last_exc if last_exc else errors.PyMongoError("Unknown read failure")

    def update(self, initial, change):
        """
        Update many documents matching 'initial' with '$set' of 'change'.
        Returns: {"matched_count": <int>, "modified_count": <int>}
        """
        if not initial or not isinstance(initial, dict):
            raise ValueError("update() requires a non-empty filter dict")
        if not change or not isinstance(change, dict):
            raise ValueError("update() requires a non-empty change dict")
        try:
            res = self.collection.update_many(initial, {"$set": change})
            logger.info(
                "update: matched=%d modified=%d",
                res.matched_count,
                res.modified_count,
            )
            return {
                "matched_count": res.matched_count,
                "modified_count": res.modified_count,
            }
        except errors.PyMongoError as e:
            logger.error("update failed: %s", e)
            raise

    def delete(self, remove):
        """
        Delete many documents matching 'remove'.
        Returns: {"deleted_count": <int>}
        """
        if not remove or not isinstance(remove, dict):
            raise ValueError("delete() requires a non-empty filter dict")
        try:
            res = self.collection.delete_many(remove)
            logger.info("delete: deleted=%d", res.deleted_count)
            return {"deleted_count": res.deleted_count}
        except errors.PyMongoError as e:
            logger.error("delete failed: %s", e)
            raise
