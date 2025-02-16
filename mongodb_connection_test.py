from pymongo import MongoClient
from pymongo.server_api import ServerApi
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBConnection:
    def __init__(self, uri: str):
        self.uri = uri
        self.client = None

    def connect(self):
        """Establish connection to MongoDB using the provided URI."""
        if not self.client:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))

    def ping(self):
        """Ping MongoDB to check if the connection is successful."""
        try:
            # Ping the server
            self.client.admin.command('ping')
            logger.info("Pinged your deployment. Successfully connected to MongoDB!")
        except Exception as e:
            logger.error(f"Error pinging MongoDB: {e}")

    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()

def main():
    uri = "mongodb+srv://lakshmibarathy:oUuou4nP2Ea0nJQm@cluster0.ttx1h.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Create a MongoDBConnection instance and perform actions
    mongo_connection = MongoDBConnection(uri)

    try:
        mongo_connection.connect()
        mongo_connection.ping()
    finally:
        mongo_connection.close()

if __name__ == "__main__":
    main()
