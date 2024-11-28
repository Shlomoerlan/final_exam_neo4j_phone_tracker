from returns.maybe import Maybe
from app.db.database import driver
from app.db.models import Device, Interaction, Location

def create_device_and_interaction(data: dict):
    with driver.session() as session:
        devices = data.get('devices', [])
        interaction = data.get('interaction', {})

        query = """
        UNWIND $devices AS device
        MERGE (d:Device {id: device.id})
        ON CREATE SET 
            d.brand = device.brand,
            d.model = device.model,
            d.name = device.name,
            d.os = device.os,
            d.latitude = device.location.latitude,
            d.longitude = device.location.longitude,
            d.altitude = device.location.altitude_meters,
            d.accuracy = device.location.accuracy_meters

        WITH device, d
        MATCH (from:Device {id: $from_device}), (to:Device {id: $to_device})
        MERGE (from)-[r:CONNECTED {
            method: $method,
            bluetooth_version: $bluetooth_version,
            signal_strength_dbm: $signal_strength_dbm,
            distance_meters: $distance_meters,
            duration_seconds: $duration_seconds,
            timestamp: $timestamp
        }]->(to)
        RETURN d
        """

        params = {
            "devices": devices,
            "from_device": interaction.get('from_device'),
            "to_device": interaction.get('to_device'),
            "method": interaction.get('method'),
            "bluetooth_version": interaction.get('bluetooth_version'),
            "signal_strength_dbm": interaction.get('signal_strength_dbm'),
            "distance_meters": interaction.get('distance_meters'),
            "duration_seconds": interaction.get('duration_seconds'),
            "timestamp": interaction.get('timestamp')
        }

        session.run(query, params)
        return {"status": "Interaction recorded"}

def count_connected_devices(device_id: str) -> int:
    query = """
    MATCH (:Device {id: $device_id})-[:CONNECTED]->(connected:Device)
    RETURN count(connected) AS connected_count
    """
    with driver.session() as session:
        result = session.run(query, {"device_id": device_id})
        record = result.single()
        if record:
            return record["connected_count"]
        return 0

def is_connected(device_id_1: str, device_id_2: str) -> bool:
    query = """
    MATCH (a:Device {id: $device_id_1})-[:CONNECTED]-(b:Device {id: $device_id_2})
    RETURN count(*) > 0 AS is_connected
    """
    with driver.session() as session:
        result = session.run(query, {"device_id_1": device_id_1, "device_id_2": device_id_2})
        record = result.single()
        return record["is_connected"] if record else False

def fetch_most_recent_interaction(device_id: str) -> dict:
    query = """
    MATCH (a:Device {id: $device_id})-[r:CONNECTED]-(b:Device)
    RETURN r, b
    ORDER BY r.timestamp DESC
    LIMIT 1
    """
    with driver.session() as session:
        result = session.run(query, {"device_id": device_id})
        record = result.single()
        if record:
            return {
                "interaction": dict(record["r"]),
                "connected_device": dict(record["b"])
            }
        return None


def find_bluetooth_connections():
    with driver.session() as session:
        query = """
        MATCH path = (d1:Device)-[r:CONNECTED*]->(d2:Device)
        WHERE all(rel IN r WHERE rel.method = 'Bluetooth')
        RETURN 
            d1.id AS from_device, 
            d2.id AS to_device, 
            length(path) AS path_length
        ORDER BY length(path) DESC
        LIMIT 1
        """
        result = session.run(query)
        return [
            {
                "from_device": record["from_device"],
                "to_device": record["to_device"],
                "path_length": record["path_length"]
            } for record in result
        ]

def find_strong_signal_connections():
    with driver.session() as session:
        query = """
        MATCH path = (d1:Device)-[r:CONNECTED]->(d2:Device)
        WHERE r.signal_strength_dbm > -60
        RETURN
            d1.id AS from_device,
            d2.id AS to_device,
            r.signal_strength_dbm AS signal_strength
        """
        result = session.run(query)
        return [
            {
                "from_device": record["from_device"],
                "to_device": record["to_device"],
                "signal_strength": record["signal_strength"]
            } for record in result
        ]

def count_device_connections(device_id):
    with driver.session() as session:
        query = """
        MATCH (d:Device {id: $device_id})-[r:CONNECTED]->()
        RETURN count(r) AS connection_count
        """
        result = session.run(query, {"device_id": device_id}).single()
        return result["connection_count"] if result else 0
