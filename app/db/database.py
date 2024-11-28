import redis
from neo4j import GraphDatabase
from app.settings.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

redis_client = redis.Redis(host="localhost",port=6379,db=0)

# neo4j db
driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER,NEO4J_PASSWORD)
)