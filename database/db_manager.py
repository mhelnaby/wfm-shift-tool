import sqlite3
import os
from datetime import datetime
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self, year=None, db_path="data"):
        self.db_path = db_path
        self.year = year or datetime.now().year
        os.makedirs(self.db_path, exist_ok=True)
        self.conn = None

    def get_connection(self):
        db_file = os.path.join(self.db_path, f"wfm_storage_{self.year}.db")
        conn = sqlite3.connect(db_file, check_same_thread=False)
        # Enforce foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def connect(self):
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        """Create all permanent tables if they don't exist."""
        with self.connect() as conn:
            cursor = conn.cursor()

            # Agents master
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agents_master (
                    citrix_uid TEXT PRIMARY KEY,
                    acd_id TEXT UNIQUE,
                    login_id TEXT,
                    name TEXT,
                    premises TEXT,
                    segment TEXT,
                    queue TEXT,
                    language TEXT,
                    batch TEXT,
                    date_of_join DATE,
                    certified_date DATE,
                    go_live_date DATE,
                    team_leader TEXT,
                    supervisor TEXT,
                    manager TEXT,
                    status TEXT DEFAULT 'Active',
                    last_working_day DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)

            # Shift dictionary
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shift_dictionary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    raw_pattern TEXT UNIQUE,
                    normalized_shift TEXT,
                    shift_type TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # User access
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_access (
                    citrix_uid TEXT PRIMARY KEY,
                    role TEXT CHECK(role IN ('OPS','RTM','ADMIN','LEAVES')),
                    full_name TEXT,
                    email TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)

            # Audit log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_citrix TEXT NOT NULL,
                    user_name TEXT,
                    action TEXT NOT NULL,
                    entity_name TEXT NOT NULL,
                    entity_key TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    ip_address TEXT,
                    session_id TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Error log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_type TEXT NOT NULL,
                    source_file TEXT,
                    source_type TEXT,
                    raw_data TEXT,
                    agent_name TEXT,
                    login_id TEXT,
                    acd_id TEXT,
                    shift_date DATE,
                    resolved BOOLEAN DEFAULT 0,
                    resolved_by TEXT,
                    resolved_at TIMESTAMP,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # LOB groups
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lob_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lob_name TEXT UNIQUE,
                    queue_names TEXT,
                    is_active BOOLEAN DEFAULT 1
                )
            """)

            # Shift swaps (moved from ensure_monthly_tables)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shift_swaps (
                    swap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requester_citrix TEXT NOT NULL,
                    agent_a_citrix TEXT NOT NULL,
                    agent_b_citrix TEXT,
                    shift_date DATE NOT NULL,
                    original_shift_a TEXT,
                    original_shift_b TEXT,
                    requested_shift_a TEXT,
                    requested_shift_b TEXT,
                    swap_type TEXT,
                    leave_type TEXT,
                    status TEXT DEFAULT 'Pending',
                    policy_violation TEXT,
                    submitted_by TEXT,
                    submitted_at TIMESTAMP,
                    reviewed_by TEXT,
                    reviewed_at TIMESTAMP,
                    review_notes TEXT,
                    FOREIGN KEY (agent_a_citrix) REFERENCES agents_master(citrix_uid),
                    FOREIGN KEY (agent_b_citrix) REFERENCES agents_master(citrix_uid)
                )
            """)

            # CMS productivity aggregated data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cms_productivity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    citrix_uid TEXT NOT NULL,
                    date DATE NOT NULL,
                    handled INTEGER DEFAULT 0,
                    talk_time INTEGER DEFAULT 0,
                    acw INTEGER DEFAULT 0,
                    hold_time INTEGER DEFAULT 0,
                    year_month TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(citrix_uid, date, year_month)
                )
            """)

            # Aspect/EIM raw events
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aspect_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    citrix_uid TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    event_type TEXT,
                    duration INTEGER DEFAULT 0,
                    aux_code TEXT,
                    year_month TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()

    def ensure_monthly_tables(self, year_month):
        """Create month-specific tables for a given year_month (e.g., '2025_01')."""
        with self.connect() as conn:
            cursor = conn.cursor()

            # Roster original
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS roster_original_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    citrix_uid TEXT NOT NULL,
                    acd_id TEXT,
                    shift_date DATE NOT NULL,
                    scheduled_shift TEXT,
                    normalized_shift TEXT,
                    shift_source TEXT DEFAULT 'Planner',
                    source_file TEXT,
                    uploaded_at TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            # Roster live (with approval tracking)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS roster_live_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    citrix_uid TEXT NOT NULL,
                    acd_id TEXT,
                    shift_date DATE NOT NULL,
                    scheduled_shift TEXT,
                    normalized_shift TEXT,
                    shift_source TEXT,
                    modified_by TEXT,
                    modified_at TIMESTAMP,
                    approved_by TEXT,
                    approved_at TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            # CMS raw data
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS cms_raw_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_date DATE NOT NULL,
                    agent_name TEXT,
                    login_id TEXT,
                    citrix_uid TEXT,
                    acd_id TEXT,
                    ans_calls INTEGER DEFAULT 0,
                    handle_time_sec INTEGER DEFAULT 0,
                    avail_time_sec INTEGER DEFAULT 0,
                    staffed_time_sec INTEGER DEFAULT 0,
                    talk_time_sec INTEGER DEFAULT 0,
                    hold_time_sec INTEGER DEFAULT 0,
                    acw_time_sec INTEGER DEFAULT 0,
                    upload_batch TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            # Aspect raw data
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS aspect_raw_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_name TEXT,
                    login_id TEXT,
                    citrix_uid TEXT,
                    acd_id TEXT,
                    event_date DATE NOT NULL,
                    login_time DATETIME,
                    logout_time DATETIME,
                    logout_reason TEXT,
                    session_duration_sec INTEGER,
                    upload_batch TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            # EIM raw data (same structure as aspect)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS eim_raw_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_name TEXT,
                    login_id TEXT,
                    citrix_uid TEXT,
                    acd_id TEXT,
                    event_date DATE NOT NULL,
                    login_time DATETIME,
                    logout_time DATETIME,
                    session_duration_sec INTEGER,
                    upload_batch TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            # Attendance processed (final output)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS attendance_processed_{year_month} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    citrix_uid TEXT NOT NULL,
                    acd_id TEXT,
                    shift_date DATE NOT NULL,
                    original_shift TEXT,
                    updated_shift TEXT,
                    staff_time_sec INTEGER DEFAULT 0,
                    staff_time_min REAL,
                    staff_time_validation TEXT,
                    attendance_status TEXT,
                    absenteeism_reason TEXT,
                    final_shift TEXT,
                    hc_status TEXT,
                    data_source TEXT,
                    confidence_score INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (citrix_uid) REFERENCES agents_master(citrix_uid)
                )
            """)

            conn.commit()

    def log_error(self, error_type, source_file, source_type, raw_data,
                  agent_name=None, login_id=None, acd_id=None, shift_date=None):
        """Convenience method to log an error."""
        with self.connect() as conn:
            conn.execute("""
                INSERT INTO error_log
                (error_type, source_file, source_type, raw_data, agent_name,
                 login_id, acd_id, shift_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (error_type, source_file, source_type, raw_data,
                  agent_name, login_id, acd_id, shift_date))
            conn.commit()