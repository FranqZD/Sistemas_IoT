import datetime
import uuid

# CREATES y SELECTS
#Creación del keyspace
CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

# Q1.1 readings_by_device_time
CREATE_READINGS_BY_DEVICE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS readings_by_device_time(
        device_id TEXT,
        device_alias TEXT,
        type TEXT,
        model TEXT,
        location TEXT,
        status TEXT,
        time DATE,
        PRIMARY KEY((device_alias), time)
    ) WITH CLUSTERING ORDER BY (time DESC)
"""
SELECT_READINGS_BY_DEVICE = """
    SELECT *
    FROM readings_by_device_time
    WHERE device_alias = ? AND time >= ?
"""

# Q1.2 devices_by_type
CREATE_DEVICES_BY_TYPE_TABLE = """
    CREATE TABLE IF NOT EXISTS devices_by_type(
        device_id TEXT,
        device_alias TEXT,
        type TEXT,
        model TEXT,
        location TEXT,
        status TEXT,
        time DATE,
        PRIMARY KEY((type), device_id)
    )
"""
SELECT_DEVICES_BY_TYPE = """
    SELECT *
    FROM devices_by_type
    WHERE type = ?
"""

# Q1.3 devices_by_status_time
CREATE_DEVICES_BY_STATUS_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS devices_by_status_time(
        device_id TEXT,
        device_alias TEXT,
        type TEXT,
        model TEXT,
        location TEXT,
        status TEXT,
        time DATE,
        PRIMARY KEY((status), time, device_id)
    )
"""
SELECT_DEVICES_BY_STATUS_TIME = """
    SELECT *
    FROM devices_by_status_time
    WHERE status = ? AND time >= ?
"""


# Q2.1 logs_by_device_time
CREATE_LOGS_BY_DEVICE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_by_device_time(
        device_id TEXT,
        device_alias TEXT,
        service_name TEXT,
        level TEXT,
        log_time DATE,
        message TEXT,
        PRIMARY KEY((device_alias), log_time)
    )
"""
SELECT_LOGS_BY_DEVICE_TIME = """
    SELECT *
    FROM logs_by_device_time
    WHERE device_alias = ? AND log_time >= ?
    ORDER BY log_time DESC
"""

# Q2.2 logs_by_level_time
CREATE_LOGS_BY_LEVEL_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_by_level_time(
        device_id TEXT,
        device_alias TEXT,
        service_name TEXT,
        level TEXT,
        log_time DATE,
        message TEXT,
        PRIMARY KEY((level), log_time, device_id)
    )
"""
SELECT_LOGS_BY_LEVEL = """
    SELECT *
    FROM logs_by_level_time
    WHERE level = ? AND log_time >= ?
"""


# Q2.3 logs_by_service_level_time
CREATE_LOGS_BY_SERVICE_LEVEL_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_by_service_level_time(
        device_id TEXT,
        device_alias TEXT,
        service_name TEXT,
        level TEXT,
        log_time DATE,
        message TEXT,
        PRIMARY KEY((service_name, level), log_time, device_id)
    )
"""
SELECT_LOGS_BY_SERVICE_LEVEL = """
    SELECT *
    FROM logs_by_service_level_time
    WHERE service_name = ? AND level = ? AND log_time >= ?
"""


# Q2.4 logs_by_service_time
CREATE_LOGS_BY_SERVICE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_by_service_time(
        device_id TEXT,
        device_alias TEXT,
        service_name TEXT,
        level TEXT,
        log_time DATE,
        message TEXT,
        PRIMARY KEY((service_name), log_time, device_id)
    )
"""
SELECT_LOGS_BY_SERVICE_TIME = """
    SELECT *
    FROM logs_by_service_time
    WHERE service_name = ? AND log_time >= ?
"""

# Q2.5 logs_count_by_device_level_time
CREATE_LOGS_COUNT_BY_DEVICE_LEVEL_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_count_by_device_level_time(
        device_id TEXT,
        device_alias TEXT,
        level TEXT,
        log_time DATE,
        count INT,
        PRIMARY KEY((device_alias, level), log_time)
    )
"""
SELECT_LOGS_COUNT_BY_DEVICE_LEVEL_TIME = """
    SELECT *
    FROM logs_count_by_device_level_time
    WHERE device_alias = ? AND level = ? AND log_time >= ?
"""

# Q3.1 alerts_by_device_time
CREATE_ALERTS_BY_DEVICE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_device_time(
        device_id TEXT,
        device_alias TEXT,
        alert_time DATE,
        metric_type TEXT,
        threshold_exceeded TEXT,
        zone_id TEXT,
        zone_alias TEXT,
        value DOUBLE,
        PRIMARY KEY((device_alias), alert_time)
    )
"""
SELECT_ALERTS_BY_DEVICE_TIME = """
    SELECT *
    FROM alerts_by_device_time
    WHERE device_alias = ? AND alert_time >= ?
    ORDER BY alert_time DESC
"""


# Q3.2 alerts_by_metric_time
CREATE_ALERTS_BY_METRIC_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_metric_time(
        device_id TEXT,
        device_alias TEXT,
        alert_time DATE,
        metric_type TEXT,
        threshold_exceeded TEXT,
        zone_id TEXT,
        zone_alias TEXT,
        value DOUBLE,
        PRIMARY KEY((metric_type), alert_time, device_id)
    )
"""
SELECT_ALERTS_BY_METRIC_TIME = """
    SELECT *
    FROM alerts_by_metric_time
    WHERE metric_type = ? AND alert_time >= ?
"""

# Q3.3 alerts_by_zone_time
CREATE_ALERTS_BY_ZONE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_zone_time(
        device_id TEXT,
        device_alias TEXT,
        alert_time DATE,
        metric_type TEXT,
        threshold_exceeded TEXT,
        zone_id TEXT,
        zone_alias TEXT,
        value DOUBLE,
        PRIMARY KEY((zone_alias), alert_time, device_id)
    )
"""
SELECT_ALERTS_BY_ZONE_TIME = """
    SELECT *
    FROM alerts_by_zone_time
    WHERE zone_alias = ? AND alert_time >= ?
"""

# Q3.4 alerts_by_metric_value_time
CREATE_ALERTS_BY_METRIC_VALUE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_metric_value_time(
        device_id TEXT,
        device_alias TEXT,
        alert_time DATE,
        metric_type TEXT,
        threshold_exceeded TEXT,
        zone_id TEXT,
        zone_alias TEXT,
        value DOUBLE,
        PRIMARY KEY((metric_type), value, alert_time, device_id)
    )
"""
SELECT_ALERTS_BY_METRIC_VALUE_TIME = """
    SELECT *
    FROM alerts_by_metric_value_time
    WHERE metric_type = ? AND value = ? AND alert_time >= ?
"""

# Función para crear keyspace
def create_keyspace(session, keyspace, replication_factor):
    query = CREATE_KEYSPACE.format(keyspace, replication_factor)
    print('Query:' + query)
    try:
        session.execute(query)
        print("Keyspace creado correctamente.")
    except Exception as e:
        print("ERROR al crear keyspace:", e)

    #session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    # Tablas de lecturas
    session.execute(CREATE_READINGS_BY_DEVICE_TIME_TABLE)
    session.execute(CREATE_DEVICES_BY_TYPE_TABLE)
    session.execute(CREATE_DEVICES_BY_STATUS_TIME_TABLE)

    # Tablas de logs
    session.execute(CREATE_LOGS_BY_DEVICE_TIME_TABLE)
    session.execute(CREATE_LOGS_BY_LEVEL_TABLE)
    session.execute(CREATE_LOGS_BY_SERVICE_LEVEL_TABLE)
    session.execute(CREATE_LOGS_BY_SERVICE_TIME_TABLE)
    session.execute(CREATE_LOGS_COUNT_BY_DEVICE_LEVEL_TIME_TABLE)

    # Tablas de alertas
    session.execute(CREATE_ALERTS_BY_DEVICE_TIME_TABLE)
    session.execute(CREATE_ALERTS_BY_METRIC_TIME_TABLE)
    session.execute(CREATE_ALERTS_BY_ZONE_TIME_TABLE)
    session.execute(CREATE_ALERTS_BY_METRIC_VALUE_TIME_TABLE)


# Funciones para hacer las lecturas
# Q1.1 readings_by_device_time
def get_readings_by_device(session, device_alias, start_date):
    stmt = session.prepare(SELECT_READINGS_BY_DEVICE)
    rows = session.execute(stmt, [device_alias, start_date])

    print(f"\n=== Readings for device_alias: {device_alias} ===")
    for row in rows:
        print(f"- Time: {row.time}")
        print(f"  Type: {row.type}")
        print(f"  Model: {row.model}")
        print(f"  Location: {row.location}")
        print(f"  Status: {row.status}")
        print(f"  Device ID: {row.device_id}")
        print()

# Q1.2 devices_by_type
def get_devices_by_type(session, sensor_type):
    stmt = session.prepare(SELECT_DEVICES_BY_TYPE)
    rows = session.execute(stmt, [sensor_type])

    print(f"\n=== Devices with type: {sensor_type} ===")
    for row in rows:
        print(f"- Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Model: {row.model}")
        print(f"  Location: {row.location}")
        print(f"  Status: {row.status}")
        print(f"  Time: {row.time}")
        print()

# Q1.3 devices_by_status_time
def get_devices_by_status_time(session, status, start_date):
    stmt = session.prepare(SELECT_DEVICES_BY_STATUS_TIME)
    rows = session.execute(stmt, [status, start_date])

    print(f"\n=== Devices with status: {status} ===")
    for row in rows:
        print(f"- Time: {row.time}")
        print(f"  Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Type: {row.type}")
        print(f"  Location: {row.location}")
        print()

# Q2.1 logs_by_device_time
def get_logs_by_device_time(session, device_alias, start_date):
    stmt = session.prepare(SELECT_LOGS_BY_DEVICE_TIME)
    rows = session.execute(stmt, [device_alias, start_date])

    print(f"\n=== Logs for device_alias: {device_alias} ===")
    for row in rows:
        print(f"- Time: {row.log_time}")
        print(f"  Level: {row.level}")
        print(f"  Service: {row.service_name}")
        print(f"  Message: {row.message}")
        print()

# Q2.2 logs_by_level_time
def get_logs_by_level(session, level, start_date):
    stmt = session.prepare(SELECT_LOGS_BY_LEVEL)
    rows = session.execute(stmt, [level, start_date])

    print(f"\n=== Logs with level: {level} ===")
    for row in rows:
        print(f"- Time: {row.log_time}")
        print(f"  Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Service: {row.service_name}")
        print(f"  Message: {row.message}")
        print()

# Q2.3 logs_by_service_level_time
def get_logs_by_service_level(session, service_name, level, start_date):
    stmt = session.prepare(SELECT_LOGS_BY_SERVICE_LEVEL)
    rows = session.execute(stmt, [service_name, level, start_date])

    print(f"\n=== Logs for service '{service_name}' with level '{level}' ===")
    for row in rows:
        print(f"- Time: {row.log_time}")
        print(f"  Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Message: {row.message}")
        print()

# Q2.4 logs_by_service_time
def get_logs_by_service(session, service_name, start_date):
    stmt = session.prepare(SELECT_LOGS_BY_SERVICE_TIME)
    rows = session.execute(stmt, [service_name, start_date])

    print(f"\n=== Logs for service: {service_name} ===")
    for row in rows:
        print(f"- Time: {row.log_time}")
        print(f"  Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Level: {row.level}")
        print(f"  Message: {row.message}")
        print()

# Q2.5 logs_count_by_device_level_time
def get_logs_count_by_device_level(session, device_alias, level, start_date):
    stmt = session.prepare(SELECT_LOGS_COUNT_BY_DEVICE_LEVEL_TIME)
    rows = session.execute(stmt, [device_alias, level, start_date])

    print(f"\n=== Logs count by device: {device_alias} ===")
    for row in rows:
        print(f"- time: {row.log_time}")
        print(f"  Device ID: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Level: {row.level}")
        print(f"  Count: {row.count}")
        print()

# Q3.1 alerts_by_device_time
def get_alerts_by_device_time(session, device_alias, start_date):
    stmt = session.prepare(SELECT_ALERTS_BY_DEVICE_TIME)
    rows = session.execute(stmt, [device_alias, start_date])

    print(f"\n=== Alerts for device_alias: {device_alias} ===")
    for row in rows:
        print(f"- Time: {row.alert_time}")
        print(f"  Metric: {row.metric_type}")
        print(f"  Threshold: {row.threshold_exceeded}")
        print(f"  Zone: {row.zone_alias}")
        print(f"  Value: {row.value}")
        print()

# Q3.2 alerts_by_metric_time
def get_alerts_by_metric_time(session, metric_type, start_date):
    stmt = session.prepare(SELECT_ALERTS_BY_METRIC_TIME)
    rows = session.execute(stmt, [metric_type, start_date])

    print(f"\n=== Alerts for metric_type: {metric_type} ===")
    for row in rows:
        print(f"- Time: {row.alert_time}")
        print(f"  Device: {row.device_id}")
        print(f"  Alias: {row.device_alias}")
        print(f"  Value: {row.value}")
        print(f"  Threshold: {row.threshold_exceeded}")
        print()

# Q3.3 alerts_by_zone_time
def get_alerts_by_zone_time(session, zone_alias, start_date):
    stmt = session.prepare(SELECT_ALERTS_BY_ZONE_TIME)
    rows = session.execute(stmt, [zone_alias, start_date])

    print(f"\n=== Alerts for zone: {zone_alias} ===")
    for row in rows:
        print(f"- Time: {row.alert_time}")
        print(f"  Device: {row.device_id}")
        print(f"  Metric: {row.metric_type}")
        print(f"  Threshold: {row.threshold_exceeded}")
        print()

# Q3.4 alerts_by_metric_value_time
def get_alerts_by_metric_value_time(session, metric_type, value, start_date):
    stmt = session.prepare(SELECT_ALERTS_BY_METRIC_VALUE_TIME)
    rows = session.execute(stmt, [metric_type, value, start_date])

    print(f"\n=== Alerts for metric '{metric_type}' with value '{value}' ===")
    for row in rows:
        print(f"- Time: {row.alert_time}")
        print(f"  Device: {row.device_id}")
        print(f"  Zone: {row.zone_alias}")
        print(f"  Threshold: {row.threshold_exceeded}")
        print()
