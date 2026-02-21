# modules/upload_handlers.py
import pandas as pd
from datetime import datetime
import re

class UploadHandler:
    def __init__(self, db, normalizer, audit):
        self.db = db
        self.normalizer = normalizer
        self.audit = audit

    def process_headcount(self, file):
        """
        معالجة ملف HC مع إزالة التكرارات والتعامل مع تعارض UNIQUE constraints.
        """
        try:
            # قراءة الملف
            df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)

            # التحقق من الأعمدة الأساسية
            required = ['Citrix UID', 'ACD ID', 'Name']
            missing = [c for c in required if c not in df.columns]
            if missing:
                return {"success": False, "error": f"Missing columns: {missing}"}

            # تنظيف البيانات: إزالة المسافات وتحويل إلى نص
            df['ACD ID'] = df['ACD ID'].astype(str).str.strip()
            df['Citrix UID'] = df['Citrix UID'].astype(str).str.strip()

            # إزالة الصفوف ذات القيم الفارغة في الأعمدة الرئيسية
            df = df.dropna(subset=['Citrix UID', 'ACD ID', 'Name'])

            # إزالة التكرارات داخل الملف على أساس Citrix UID ثم ACD ID
            before = len(df)
            df = df.drop_duplicates(subset=['Citrix UID'], keep='first')
            df = df.drop_duplicates(subset=['ACD ID'], keep='first')
            removed = before - len(df)
            if removed > 0:
                print(f"Removed {removed} duplicate rows (based on Citrix UID and ACD ID).")

            updated = 0
            new = 0
            errors = []

            with self.db.connect() as conn:
                for _, row in df.iterrows():
                    citrix = row['Citrix UID']
                    acd = row['ACD ID']
                    name = row['Name']

                    # تجهيز البيانات
                    data = {
                        'acd_id': acd,
                        'name': name,
                        'premises': row.get('Premises'),
                        'segment': row.get('Segment'),
                        'queue': row.get('Queue'),
                        'language': row.get('Language'),
                        'batch': row.get('Batch'),
                        'date_of_join': row.get('Date of Join'),
                        'certified_date': row.get('Certified Date'),
                        'go_live_date': row.get('Go Live Date'),
                        'team_leader': row.get('Team Leaders'),
                        'supervisor': row.get('Supervisor'),
                        'manager': row.get('Manger'),
                        'status': row.get('Status', 'Active'),
                    }

                    # تحويل القيم الفارغة إلى None
                    for k, v in data.items():
                        if pd.isna(v):
                            data[k] = None

                    columns = ','.join(data.keys())
                    placeholders = ','.join(['?' for _ in data])
                    values = [citrix] + list(data.values())

                    # التحقق مما إذا كان ACD ID موجوداً بالفعل لشخص آخر
                    existing = conn.execute(
                        "SELECT citrix_uid FROM agents_master WHERE acd_id = ? AND citrix_uid != ?",
                        (acd, citrix)
                    ).fetchone()
                    if existing:
                        errors.append(f"ACD ID {acd} already assigned to {existing['citrix_uid']}, skipping {citrix}")
                        continue

                    # محاولة الإدراج أو التحديث باستخدام ON CONFLICT على citrix_uid
                    conn.execute(f"""
                        INSERT INTO agents_master (citrix_uid, {columns}) 
                        VALUES (?, {placeholders})
                        ON CONFLICT(citrix_uid) DO UPDATE SET 
                            acd_id = excluded.acd_id,
                            name = excluded.name,
                            premises = excluded.premises,
                            segment = excluded.segment,
                            queue = excluded.queue,
                            language = excluded.language,
                            batch = excluded.batch,
                            date_of_join = excluded.date_of_join,
                            certified_date = excluded.certified_date,
                            go_live_date = excluded.go_live_date,
                            team_leader = excluded.team_leader,
                            supervisor = excluded.supervisor,
                            manager = excluded.manager,
                            status = excluded.status,
                            updated_at = CURRENT_TIMESTAMP
                    """, values)

                    # يمكن تحديث العداد ببساطة
                    new += 1

                conn.commit()

            result = {"success": True, "agents_updated": updated, "new_agents": new}
            if errors:
                result["warnings"] = errors
            return result

        except Exception as e:
            return {"success": False, "error": str(e)}

    def process_roster(self, file, mapping, year_month):
        """
        معالجة ملف Roster (جدول المناوبات) باستخدام mapping المحدد.
        """
        try:
            # قراءة الملف
            if file.name.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            name_col = mapping['name_col']
            citrix_col = mapping.get('citrix_col')
            acd_col = mapping.get('acd_col')
            login_col = mapping.get('login_col')
            date_cols = mapping['date_cols']

            # التحقق من وجود الأعمدة
            if name_col not in df.columns:
                return {"success": False, "error": f"Column '{name_col}' not found."}
            for col in date_cols:
                if col not in df.columns:
                    return {"success": False, "error": f"Date column '{col}' not found."}

            # قائمة الأعمدة الثابتة (للربط)
            id_vars = [name_col]
            if citrix_col and citrix_col != 'None' and citrix_col in df.columns:
                id_vars.append(citrix_col)
            if acd_col and acd_col != 'None' and acd_col in df.columns:
                id_vars.append(acd_col)
            if login_col and login_col != 'None' and login_col in df.columns:
                id_vars.append(login_col)

            # تحويل الجدول من العرض العريض إلى الطولي
            melted = df.melt(id_vars=id_vars, value_vars=date_cols,
                             var_name='raw_date', value_name='raw_shift')

            # تحويل التاريخ (تنسيق مثل 21-Feb-26)
            try:
                melted['shift_date'] = pd.to_datetime(melted['raw_date'], format='%d-%b-%y', errors='coerce')
            except:
                melted['shift_date'] = pd.to_datetime(melted['raw_date'], errors='coerce')
            melted = melted.dropna(subset=['shift_date'])

            # تطبيع الشفتات
            melted['normalized_shift'] = melted['raw_shift'].apply(self.normalizer.normalize)

            unknown_agents = []
            records = []

            with self.db.connect() as conn:
                # بناء قاموس للربط بين الاسم / acd_id / login_id و citrix_uid
                agent_map = {}
                cur = conn.execute("SELECT citrix_uid, acd_id, name FROM agents_master")
                for row in cur:
                    if row['citrix_uid']:
                        agent_map[row['citrix_uid'].strip()] = row['citrix_uid']
                    if row['name']:
                        agent_map[row['name'].strip()] = row['citrix_uid']
                    if row['acd_id']:
                        agent_map[str(row['acd_id']).strip()] = row['citrix_uid']

                for _, row in melted.iterrows():
                    citrix = None
                    # محاولة استخراج citrix_uid بالترتيب
                    if citrix_col and citrix_col != 'None' and pd.notna(row.get(citrix_col)):
                        citrix = agent_map.get(str(row[citrix_col]).strip())
                    if not citrix and acd_col and acd_col != 'None' and pd.notna(row.get(acd_col)):
                        citrix = agent_map.get(str(row[acd_col]).strip())
                    if not citrix and login_col and login_col != 'None' and pd.notna(row.get(login_col)):
                        citrix = agent_map.get(str(row[login_col]).strip())
                    if not citrix and pd.notna(row[name_col]):
                        citrix = agent_map.get(str(row[name_col]).strip())

                    if not citrix:
                        unknown_agents.append(str(row[name_col]))
                        continue

                    records.append((
                        citrix,
                        row.get(acd_col) if acd_col and acd_col != 'None' else None,
                        row['shift_date'].date(),
                        row['raw_shift'],
                        row['normalized_shift'],
                        file.name
                    ))

                if records:
                    # إدراج في جدول roster_original
                    conn.executemany(f"""
                        INSERT INTO roster_original_{year_month}
                        (citrix_uid, acd_id, shift_date, scheduled_shift, normalized_shift, source_file)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, records)

                    # إدراج في جدول roster_live (نفس البيانات في البداية)
                    conn.executemany(f"""
                        INSERT INTO roster_live_{year_month}
                        (citrix_uid, acd_id, shift_date, scheduled_shift, normalized_shift, shift_source, source_file)
                        VALUES (?, ?, ?, ?, ?, 'Planner', ?)
                    """, records)

                    conn.commit()

            return {
                "success": True,
                "rows_processed": len(records),
                "unknown_agents": unknown_agents
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
def process_cms_productivity(self, file, year_month):
    """
    Process a CMS productivity report and insert into cms_raw_{year_month}.
    Expected columns (adjust to your file):
        - Date / Report Date
        - Agent ID / Citrix UID / ACD ID
        - Handled / Ans Calls
        - Talk Time / Handle Time (seconds)
        - Available Time / Staffed Time
        - Hold Time
        - ACW Time
    """
    try:
        # Read file (supports CSV or Excel)
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        # Normalize column names: lowercase, replace spaces with underscores
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]

        # Map columns to database fields (adjust as needed)
        column_mapping = {
            'date': 'report_date',          # your file's date column
            'agent_id': 'citrix_uid',       # or 'citrix_uid', 'acd_id', etc.
            'handled': 'ans_calls',          # number of answered calls
            'talk_time': 'handle_time_sec',  # talk time in seconds
            'avail_time': 'avail_time_sec',  # available time
            'staffed_time': 'staffed_time_sec',
            'hold_time': 'hold_time_sec',
            'acw_time': 'acw_time_sec'
        }

        unknown_agents = []
        rows_processed = 0
        upload_batch = datetime.now().strftime('%Y%m%d%H%M%S')

        # For monthly table name
        table_name = f"cms_raw_{year_month}"

        for _, row in df.iterrows():
            # Extract agent identifier (try common column names)
            agent_id = None
            for col in ['citrix_uid', 'agent_id', 'acd_id', 'login_id']:
                if col in row and pd.notna(row[col]):
                    agent_id = str(row[col])
                    break
            if not agent_id:
                continue

            # Verify agent exists in agents_master
            with self.db.connect() as conn:
                cur = conn.execute(
                    "SELECT citrix_uid FROM agents_master WHERE citrix_uid = ? OR acd_id = ? OR login_id = ?",
                    (agent_id, agent_id, agent_id)
                )
                result = cur.fetchone()
                if result:
                    # Use the actual citrix_uid from master if found
                    actual_citrix = result['citrix_uid']
                else:
                    unknown_agents.append(agent_id)
                    continue

            # Parse date (assume a date column exists)
            date_val = None
            for dcol in ['date', 'report_date', 'day']:
                if dcol in row and pd.notna(row[dcol]):
                    date_val = row[dcol]
                    break
            if date_val is None:
                continue
            # Convert to date object
            if isinstance(date_val, str):
                try:
                    date_obj = datetime.strptime(date_val, '%Y-%m-%d').date()
                except:
                    date_obj = pd.to_datetime(date_val).date()
            else:
                date_obj = pd.to_datetime(date_val).date()

            # Extract numeric fields (default to 0)
            ans_calls = int(row.get('handled', 0) or 0)
            handle_time = int(row.get('talk_time', 0) or 0)
            avail_time = int(row.get('avail_time', 0) or 0)
            staffed_time = int(row.get('staffed_time', 0) or 0)
            hold_time = int(row.get('hold_time', 0) or 0)
            acw_time = int(row.get('acw_time', 0) or 0)

            # Insert into monthly table
            with self.db.connect() as conn:
                conn.execute(f"""
                    INSERT INTO {table_name}
                    (report_date, citrix_uid, ans_calls, handle_time_sec,
                     avail_time_sec, staffed_time_sec, hold_time_sec,
                     acw_time_sec, upload_batch, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (date_obj, actual_citrix, ans_calls, handle_time,
                      avail_time, staffed_time, hold_time, acw_time, upload_batch))
                rows_processed += 1

        return {
            'success': True,
            'rows_processed': rows_processed,
            'unknown_agents': list(set(unknown_agents))[:10]   # limit to first 10
        }

    except Exception as e:
        # Optionally log the error to your error_log table
        # self.db.log_error(...)
        return {'success': False, 'error': str(e)}
def process_aspect_eim(self, file, year_month):
    """
    Process an Aspect or EIM event report and insert into aspect_raw_{year_month}
    or eim_raw_{year_month} depending on the file type.
    Expected columns:
        - Agent Name / Login ID / Citrix UID
        - Event Date
        - Login Time / Logout Time
        - Logout Reason
        - Session Duration
    """
    try:
        # Determine if it's Aspect or EIM (you could detect by filename or content)
        # For simplicity, assume you pass a parameter; here we'll just use 'aspect_raw'
        # But you might want to check file.name to decide.
        if 'eim' in file.name.lower():
            table_name = f"eim_raw_{year_month}"
        else:
            table_name = f"aspect_raw_{year_month}"

        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]

        unknown_agents = []
        rows_processed = 0
        upload_batch = datetime.now().strftime('%Y%m%d%H%M%S')

        for _, row in df.iterrows():
            # Identify agent
            agent_id = None
            for col in ['citrix_uid', 'agent_id', 'acd_id', 'login_id']:
                if col in row and pd.notna(row[col]):
                    agent_id = str(row[col])
                    break
            if not agent_id:
                # Try agent_name if present (but need mapping)
                continue

            with self.db.connect() as conn:
                cur = conn.execute(
                    "SELECT citrix_uid FROM agents_master WHERE citrix_uid = ? OR acd_id = ? OR login_id = ?",
                    (agent_id, agent_id, agent_id)
                )
                result = cur.fetchone()
                if result:
                    actual_citrix = result['citrix_uid']
                else:
                    unknown_agents.append(agent_id)
                    continue

            # Event date
            date_val = None
            for dcol in ['event_date', 'date', 'day']:
                if dcol in row and pd.notna(row[dcol]):
                    date_val = row[dcol]
                    break
            if date_val is None:
                continue
            if isinstance(date_val, str):
                date_obj = datetime.strptime(date_val, '%Y-%m-%d').date()
            else:
                date_obj = pd.to_datetime(date_val).date()

            # Login and logout times
            login_time = None
            logout_time = None
            for tcol in ['login_time', 'login']:
                if tcol in row and pd.notna(row[tcol]):
                    login_time = row[tcol]
                    break
            for tcol in ['logout_time', 'logout']:
                if tcol in row and pd.notna(row[tcol]):
                    logout_time = row[tcol]
                    break

            # Convert to datetime if needed
            if login_time and isinstance(login_time, str):
                try:
                    login_dt = datetime.strptime(login_time, '%Y-%m-%d %H:%M:%S')
                except:
                    login_dt = pd.to_datetime(login_time).to_pydatetime()
            else:
                login_dt = login_time

            if logout_time and isinstance(logout_time, str):
                try:
                    logout_dt = datetime.strptime(logout_time, '%Y-%m-%d %H:%M:%S')
                except:
                    logout_dt = pd.to_datetime(logout_time).to_pydatetime()
            else:
                logout_dt = logout_time

            logout_reason = row.get('logout_reason', '') or ''
            session_duration = int(row.get('session_duration_sec', 0) or 0)

            # Insert
            with self.db.connect() as conn:
                conn.execute(f"""
                    INSERT INTO {table_name}
                    (agent_name, login_id, citrix_uid, acd_id, event_date,
                     login_time, logout_time, logout_reason, session_duration_sec,
                     upload_batch, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    row.get('agent_name', ''), row.get('login_id', ''),
                    actual_citrix, row.get('acd_id', ''),
                    date_obj, login_dt, logout_dt, logout_reason,
                    session_duration, upload_batch
                ))
                rows_processed += 1

        return {
            'success': True,
            'rows_processed': rows_processed,
            'unknown_agents': list(set(unknown_agents))[:10]
        }

    except Exception as e:
        return {'success': False, 'error': str(e)}