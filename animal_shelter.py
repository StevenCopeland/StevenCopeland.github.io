from pymongo import MongoClient, errors
import logging

logger = logging.getLogger(__name__)


class AnimalShelter:
    """
    MongoDB data access for the dashboard.
    """

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

    def create(self, data):
        """
        Insert one document.
        Returns: {"inserted_id": <str>, "acknowledged": <bool>}
        """
        if not data or not isinstance(data, dict):
            raise ValueError("create() requires a non-empty dict")
        try:
            result = self.collection.insert_one(data)
            logger.info("create: inserted_id=%s acknowledged=%s",
                        result.inserted_id, result.acknowledged)
            return {
                "inserted_id": str(result.inserted_id),
                "acknowledged": result.acknowledged
            }
        except errors.PyMongoError as e:
            logger.error("create failed: %s", e)
            raise

    def read(self, query=None, projection=None, limit=None):
        """
        Read documents with optional query, projection, and limit.
        Returns: list[dict]
        """
        query = query or {}
        try:
            cursor = self.collection.find(query, projection)
            if limit is not None:
                cursor = cursor.limit(limit)
            docs = list(cursor)
            logger.info("read: %d docs", len(docs))
            return docs
        except errors.PyMongoError as e:
            logger.error("read failed: %s", e)
            raise

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
            logger.info("update: matched=%d modified=%d",
                        res.matched_count, res.modified_count)
            return {
                "matched_count": res.matched_count,
                "modified_count": res.modified_count
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
