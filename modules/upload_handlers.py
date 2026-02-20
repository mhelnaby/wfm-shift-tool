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
        try:
            df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
            # Detect columns – you'll need to adapt to your HC format
            # Expected columns: Citrix UID, ACD ID, Name, Queue, Team Leader, etc.
            # We'll assume a standard mapping; in production you'd have a mapping UI.
            required = ['Citrix UID', 'ACD ID', 'Name']
            missing = [c for c in required if c not in df.columns]
            if missing:
                return {"success": False, "error": f"Missing columns: {missing}"}

            updated = 0
            new = 0
            with self.db.connect() as conn:
                for _, row in df.iterrows():
                    citrix = row['Citrix UID']
                    acd = row['ACD ID']
                    name = row['Name']
                    # Check if exists
                    cur = conn.execute("SELECT citrix_uid FROM agents_master WHERE citrix_uid = ?", (citrix,))
                    exists = cur.fetchone()
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
                        'team_leader': row.get('Team Leader'),
                        'supervisor': row.get('Supervisor'),
                        'manager': row.get('Manager'),
                    }
                    if exists:
                        # Update
                        set_clause = ', '.join([f"{k}=?" for k in data.keys()])
                        values = list(data.values()) + [citrix]
                        conn.execute(f"UPDATE agents_master SET {set_clause} WHERE citrix_uid=?", values)
                        updated += 1
                    else:
                        # Insert
                        cols = ','.join(data.keys())
                        placeholders = ','.join(['?' for _ in data])
                        values = list(data.values())
                        conn.execute(f"INSERT INTO agents_master (citrix_uid, {cols}) VALUES (?, {placeholders})", [citrix] + values)
                        new += 1
                conn.commit()
            return {"success": True, "agents_updated": updated, "new_agents": new}
        except Exception as e:
            return {"success": False, "error": str(e)}
                def process_roster(self, file, mapping):
        try:
            df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
            # mapping contains: name_col, acd_id_col, citrix_uid_col, date_cols, date_format
            name_col = mapping['name']
            acd_col = mapping.get('acd_id')
            citrix_col = mapping.get('citrix_uid')
            login_id_col = mapping.get('login_id')
            date_cols = mapping['date_cols']
            date_format = mapping.get('date_format', '%d-%b')

            # Melt
            id_vars = [name_col]
            if acd_col:
                id_vars.append(acd_col)
            if citrix_col:
                id_vars.append(citrix_col)
            if login_id_col:
                id_vars.append(login_id_col)

            melted = df.melt(id_vars=id_vars, value_vars=date_cols,
                             var_name='raw_date', value_name='raw_shift')
            melted['shift_date'] = pd.to_datetime(melted['raw_date'], format=date_format, errors='coerce')
            melted = melted.dropna(subset=['shift_date'])

            # Normalize shifts
            melted['normalized'] = melted['raw_shift'].apply(self.normalizer.normalize)

            # Map to citrix_uid
            unknown_agents = []
            with self.db.connect() as conn:
                # Load agent mapping
                agent_map = {}
                cur = conn.execute("SELECT acd_id, citrix_uid, name FROM agents_master")
                for row in cur:
                    agent_map[row['acd_id']] = row['citrix_uid']
                    agent_map[row['name']] = row['citrix_uid']  # fallback by name

                records = []
                for _, row in melted.iterrows():
                    citrix = None
                    if citrix_col and row.get(citrix_col):
                        citrix = row[citrix_col]
                    elif acd_col and row.get(acd_col):
                        citrix = agent_map.get(row[acd_col])
                    elif row[name_col] in agent_map:
                        citrix = agent_map[row[name_col]]
                    if not citrix:
                        unknown_agents.append(row[name_col])
                        continue
                    records.append((
                        citrix,
                        row.get(acd_col),
                        row['shift_date'].date(),
                        row['raw_shift'],
                        row['normalized'],
                        file.name
                    ))

                # Insert into roster_original
                year_month = f"{pd.Timestamp.now().year}_{pd.Timestamp.now().month:02d}"  # simplify
                self.db.ensure_monthly_tables(year_month)
                conn.executemany(f"""
                    INSERT INTO roster_original_{year_month}
                    (citrix_uid, acd_id, shift_date, scheduled_shift, normalized_shift, source_file)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, records)
                # Also copy to roster_live (initially same)
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

                def process_cms_report(self, file, report_date, has_header=True):
        try:
            content = file.getvalue().decode('utf-8').splitlines()
            # Skip header if present
            start = 1 if has_header else 0
            records = []
            for line in content[start:]:
                if not line.strip():
                    continue
                parts = line.split('\t')
                if len(parts) < 10:
                    continue
                # Expected: Date, Name, Login ID, Ans Calls, Handle Time, ...
                # Adapt based on your actual columns
                try:
                    agent_name = parts[1].strip()
                    login_id = parts[2].strip()
                    ans_calls = int(parts[3]) if parts[3].strip() else 0
                    handle_time = float(parts[4]) if parts[4].strip() else 0
                    avail_time = float(parts[5]) if parts[5].strip() else 0
                    staffed_time = float(parts[6]) if parts[6].strip() else 0
                    talk_time = float(parts[9]) if parts[9].strip() else 0
                    hold_time = float(parts[14]) if len(parts) > 14 and parts[14].strip() else 0
                    acw_time = float(parts[15]) if len(parts) > 15 and parts[15].strip() else 0

                    # Map to citrix_uid via login_id or name
                    with self.db.connect() as conn:
                        cur = conn.execute(
                            "SELECT citrix_uid, acd_id FROM agents_master WHERE login_id = ? OR name = ?",
                            (login_id, agent_name)
                        )
                        agent = cur.fetchone()
                        citrix = agent['citrix_uid'] if agent else None
                        acd = agent['acd_id'] if agent else None

                    records.append((
                        report_date,
                        agent_name,
                        login_id,
                        citrix,
                        acd,
                        ans_calls,
                        handle_time,
                        avail_time,
                        staffed_time,
                        talk_time,
                        hold_time,
                        acw_time,
                        file.name
                    ))
                except Exception as e:
                    # log error but continue
                    pass

            if records:
                year_month = f"{report_date.year}_{report_date.month:02d}"
                self.db.ensure_monthly_tables(year_month)
                with self.db.connect() as conn:
                    conn.executemany(f"""
                        INSERT INTO cms_raw_{year_month}
                        (report_date, agent_name, login_id, citrix_uid, acd_id,
                         ans_calls, handle_time_sec, avail_time_sec, staffed_time_sec,
                         talk_time_sec, hold_time_sec, acw_time_sec, upload_batch)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, records)
                    conn.commit()
            return {"success": True, "records": len(records)}
        except Exception as e:
            return {"success": False, "error": str(e)}
            