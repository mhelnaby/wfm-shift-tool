# modules/upload_handlers.py
import pandas as pd
from datetime import datetime
import hashlib

class UploadHandler:
    def __init__(self, db, normalizer, audit):
        self.db = db
        self.normalizer = normalizer
        self.audit = audit

    # ---------- Helper methods ----------
    def _get_agent_by_login(self, login_id):
        """ابحث عن الوكيل باستخدام login_id (أو acd_id) وأعد citrix_uid و acd_id."""
        if not login_id or login_id == '0' or login_id == 'Totals':
            return None
        with self.db.connect() as conn:
            # حاول بـ login_id أولاً
            cur = conn.execute(
                "SELECT citrix_uid, acd_id FROM agents_master WHERE login_id = ?",
                (str(login_id).strip(),)
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            # إذا لم نجد، جرب acd_id
            cur = conn.execute(
                "SELECT citrix_uid, acd_id FROM agents_master WHERE acd_id = ?",
                (str(login_id).strip(),)
            )
            row = cur.fetchone()
            if row:
                return dict(row)
        return None

    # ---------- Headcount ----------
    def process_headcount(self, file):
        """معالجة ملف HC (بدون تغيير)"""
        try:
            df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
            required = ['Citrix UID', 'ACD ID', 'Name']
            missing = [c for c in required if c not in df.columns]
            if missing:
                return {"success": False, "error": f"Missing columns: {missing}"}

            df['ACD ID'] = df['ACD ID'].astype(str).str.strip()
            df['Citrix UID'] = df['Citrix UID'].astype(str).str.strip()
            df = df.dropna(subset=['Citrix UID', 'ACD ID', 'Name'])

            # إزالة التكرارات
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

                    for k, v in data.items():
                        if pd.isna(v):
                            data[k] = None

                    columns = ','.join(data.keys())
                    placeholders = ','.join(['?' for _ in data])
                    values = [citrix] + list(data.values())

                    existing = conn.execute(
                        "SELECT citrix_uid FROM agents_master WHERE acd_id = ? AND citrix_uid != ?",
                        (acd, citrix)
                    ).fetchone()
                    if existing:
                        errors.append(f"ACD ID {acd} already assigned to {existing['citrix_uid']}, skipping {citrix}")
                        continue

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

                    new += 1

                conn.commit()

            result = {"success": True, "agents_updated": updated, "new_agents": new}
            if errors:
                result["warnings"] = errors
            return result

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- Roster ----------
    def process_roster(self, file, mapping, year_month):
        """معالجة ملف Roster (جدول المناوبات)"""
        try:
            if file.name.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            name_col = mapping['name_col']
            citrix_col = mapping.get('citrix_col')
            acd_col = mapping.get('acd_col')
            login_col = mapping.get('login_col')
            date_cols = mapping['date_cols']

            if name_col not in df.columns:
                return {"success": False, "error": f"Column '{name_col}' not found."}
            for col in date_cols:
                if col not in df.columns:
                    return {"success": False, "error": f"Date column '{col}' not found."}

            id_vars = [name_col]
            if citrix_col and citrix_col != 'None' and citrix_col in df.columns:
                id_vars.append(citrix_col)
            if acd_col and acd_col != 'None' and acd_col in df.columns:
                id_vars.append(acd_col)
            if login_col and login_col != 'None' and login_col in df.columns:
                id_vars.append(login_col)

            melted = df.melt(id_vars=id_vars, value_vars=date_cols,
                             var_name='raw_date', value_name='raw_shift')

            try:
                melted['shift_date'] = pd.to_datetime(melted['raw_date'], format='%d-%b-%y', errors='coerce')
            except:
                melted['shift_date'] = pd.to_datetime(melted['raw_date'], errors='coerce')
            melted = melted.dropna(subset=['shift_date'])

            melted['normalized_shift'] = melted['raw_shift'].apply(self.normalizer.normalize)

            unknown_agents = []
            records = []

            with self.db.connect() as conn:
                agent_map = {}
                cur = conn.execute("SELECT citrix_uid, acd_id, login_id, name FROM agents_master")
                for row in cur:
                    if row['citrix_uid']:
                        agent_map[row['citrix_uid'].strip()] = row['citrix_uid']
                    if row['name']:
                        agent_map[row['name'].strip()] = row['citrix_uid']
                    if row['acd_id']:
                        agent_map[str(row['acd_id']).strip()] = row['citrix_uid']
                    if row['login_id']:
                        agent_map[str(row['login_id']).strip()] = row['citrix_uid']

                for _, row in melted.iterrows():
                    citrix = None
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
                    conn.executemany(f"""
                        INSERT INTO roster_original_{year_month}
                        (citrix_uid, acd_id, shift_date, scheduled_shift, normalized_shift, source_file)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, records)

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

    # ---------- CMS Productivity ----------
    def process_cms_productivity(self, file, year_month):
        """
        معالجة ملف Agents Productivity Report.
        يتوقع ملف نصي مفصول بعلامة تبويب ويبدأ بسطر عنوان ثم سطر رأس.
        """
        try:
            if file.name.endswith('.csv') or file.name.endswith('.txt'):
                df = pd.read_csv(file, sep='\t', skiprows=1, encoding='utf-8')
            else:
                df = pd.read_excel(file, skiprows=1)

            df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]

            required = ['date', 'login_id', 'ans_calls', 'handle_time', 'talk_time', 'hold_time', 'acw_time']
            missing = [c for c in required if c not in df.columns]
            if missing:
                return {"success": False, "error": f"Missing columns: {missing}"}

            batch_id = hashlib.md5(f"{datetime.now()}{file.name}".encode()).hexdigest()[:10]
            inserted = 0
            unknown_logins = set()
            errors = []

            with self.db.connect() as conn:
                for idx, row in df.iterrows():
                    login_id = str(row['login_id']).strip()
                    if not login_id or login_id in ['0', 'Totals']:
                        continue

                    agent = self._get_agent_by_login(login_id)
                    if not agent:
                        unknown_logins.add(login_id)
                        continue

                    citrix_uid = agent['citrix_uid']
                    acd_id = agent.get('acd_id')

                    # تحويل التاريخ
                    try:
                        date_val = row['date']
                        if isinstance(date_val, str):
                            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%b-%y'):
                                try:
                                    report_date = datetime.strptime(date_val, fmt).date()
                                    break
                                except:
                                    continue
                            else:
                                report_date = pd.to_datetime(date_val).date()
                        else:
                            report_date = pd.to_datetime(date_val).date()
                    except Exception as e:
                        errors.append(f"Row {idx}: invalid date {row['date']}")
                        continue

                    def to_int(val):
                        if pd.isna(val):
                            return 0
                        try:
                            return int(float(str(val).replace(',', '')))
                        except:
                            return 0

                    ans_calls = to_int(row.get('ans_calls', 0))
                    handle_time = to_int(row.get('handle_time', 0))
                    talk_time = to_int(row.get('talk_time', 0))
                    hold_time = to_int(row.get('hold_time', 0))
                    acw_time = to_int(row.get('acw_time', 0))
                    avail_time = to_int(row.get('avail_time', 0))
                    staffed_time = to_int(row.get('staffed_time', 0))

                    conn.execute(f"""
                        INSERT INTO cms_raw_{year_month}
                        (report_date, agent_name, login_id, citrix_uid, acd_id,
                         ans_calls, handle_time_sec, avail_time_sec, staffed_time_sec,
                         talk_time_sec, hold_time_sec, acw_time_sec, upload_batch)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        report_date,
                        row.get('name', ''),
                        login_id,
                        citrix_uid,
                        acd_id,
                        ans_calls,
                        handle_time,
                        avail_time,
                        staffed_time,
                        talk_time,
                        hold_time,
                        acw_time,
                        batch_id
                    ))
                    inserted += 1

                conn.commit()

            return {
                "success": True,
                "rows_processed": inserted,
                "unknown_agents": list(unknown_logins)[:10],
                "warnings": errors if errors else None
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- Aspect / EIM ----------
    def process_aspect(self, file, year_month):
        return self._process_login_logout(file, year_month, table_prefix='aspect_raw')

    def process_eim(self, file, year_month):
        return self._process_login_logout(file, year_month, table_prefix='eim_raw')

    def _process_login_logout(self, file, year_month, table_prefix):
        """
        معالجة ملفات تسجيل الدخول/الخروج (Aspect/EIM).
        تتعامل مع ملفات EIM و Login-Logout بشكل موحد.
        """
        try:
            # قراءة الملف كله للبحث عن الرأس
            content = file.read().decode('utf-8', errors='ignore')
            file.seek(0)
            lines = content.splitlines()

            # البحث عن سطر الرأس الذي يحتوي على Agent Name و Date
            header_idx = None
            for i, line in enumerate(lines[:20]):  # نبحث في أول 20 سطر فقط
                if 'Agent Name' in line and 'Date' in line:
                    header_idx = i
                    break
                if 'Agent name' in line.lower() and 'date' in line.lower():
                    header_idx = i
                    break

            # إذا لم نجد، نبحث عن أي سطر يحتوي على Login ID أو Agent
            if header_idx is None:
                for i, line in enumerate(lines[:10]):
                    if 'Login ID' in line or 'Agent' in line:
                        header_idx = i
                        break

            # إذا لم نجد، نفترض أن الرأس في السطر الأول
            if header_idx is None:
                header_idx = 0

            # إعادة قراءة الملف باستخدام pandas مع التخطي حتى الرأس
            # نستخدم sep=None للكشف التلقائي عن الفاصل، و on_bad_lines='skip' لتجاهل الصفوف التالفة
            df = pd.read_csv(
                file,
                sep=None,
                engine='python',
                skiprows=header_idx,
                encoding='utf-8',
                on_bad_lines='skip'
            )

            # تنظيف أسماء الأعمدة: إلى lower case وإزالة المسافات
            df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]

            # التعامل مع الأعمدة المكررة (مثل login_time.1, login_time.2)
            cols = df.columns.tolist()
            seen = {}
            unique_cols = []
            for col in cols:
                base = col.split('.')[0]
                if base not in seen:
                    seen[base] = True
                    unique_cols.append(col)
                else:
                    # نتجاهل الأعمدة المكررة
                    pass
            df = df[unique_cols]

            # تعيين event_date من العمود date إذا وجد
            if 'date' in df.columns:
                df.rename(columns={'date': 'event_date'}, inplace=True)

            # تعيين login_id
            if 'login_id' not in df.columns:
                # البحث عن عمود يشبه login
                for col in df.columns:
                    if 'login' in col:
                        df.rename(columns={col: 'login_id'}, inplace=True)
                        break
                else:
                    # إذا لم نجد، نبحث عن أي عمود id
                    for col in df.columns:
                        if 'id' in col:
                            df.rename(columns={col: 'login_id'}, inplace=True)
                            break

            # التحقق من وجود الأعمدة الأساسية
            required = ['login_id', 'event_date']
            missing = [r for r in required if r not in df.columns]
            if missing:
                return {"success": False, "error": f"Missing columns: {missing}"}

            # إزالة الصفوف ذات login_id فارغ
            df = df.dropna(subset=['login_id'])
            df['login_id'] = df['login_id'].astype(str).str.strip()

            batch_id = hashlib.md5(f"{datetime.now()}{file.name}".encode()).hexdigest()[:10]
            inserted = 0
            unknown_logins = set()
            errors = []

            with self.db.connect() as conn:
                for idx, row in df.iterrows():
                    login_id = row['login_id']
                    if not login_id or login_id in ['0', 'Totals']:
                        continue

                    agent = self._get_agent_by_login(login_id)
                    if not agent:
                        unknown_logins.add(login_id)
                        continue

                    citrix_uid = agent['citrix_uid']
                    acd_id = agent.get('acd_id')

                    # تاريخ الحدث
                    try:
                        date_val = row['event_date']
                        if isinstance(date_val, str):
                            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%b-%y'):
                                try:
                                    event_date = datetime.strptime(date_val, fmt).date()
                                    break
                                except:
                                    continue
                            else:
                                event_date = pd.to_datetime(date_val).date()
                        else:
                            event_date = pd.to_datetime(date_val).date()
                    except Exception as e:
                        errors.append(f"Row {idx}: invalid event_date {row.get('event_date')}")
                        continue

                    # أوقات الدخول والخروج
                    login_time = row.get('login_time')
                    logout_time = row.get('logout_time')
                    logout_date_str = row.get('logout_date')

                    login_dt = None
                    logout_dt = None
                    duration = 0

                    if login_time and login_time != '0' and pd.notna(login_time):
                        try:
                            if isinstance(login_time, str):
                                login_dt = datetime.strptime(login_time.strip(), '%I:%M%p')
                                login_dt = login_dt.replace(year=event_date.year, month=event_date.month, day=event_date.day)
                            else:
                                login_dt = pd.to_datetime(login_time).to_pydatetime()
                        except:
                            pass

                    if logout_time and logout_time != '0' and pd.notna(logout_time):
                        try:
                            logout_date = event_date
                            if logout_date_str and pd.notna(logout_date_str):
                                try:
                                    logout_date = datetime.strptime(str(logout_date_str).strip(), '%d/%m/%Y').date()
                                except:
                                    pass
                            if isinstance(logout_time, str):
                                logout_dt = datetime.strptime(logout_time.strip(), '%I:%M%p')
                                logout_dt = logout_dt.replace(year=logout_date.year, month=logout_date.month, day=logout_date.day)
                            else:
                                logout_dt = pd.to_datetime(logout_time).to_pydatetime()
                        except:
                            pass

                    if login_dt and logout_dt:
                        duration = int((logout_dt - login_dt).total_seconds())
                        if duration < 0:
                            duration = 0

                    logout_reason = row.get('logout_reason', '')

                    conn.execute(f"""
                        INSERT INTO {table_prefix}_{year_month}
                        (agent_name, login_id, citrix_uid, acd_id, event_date,
                         login_time, logout_time, logout_reason, session_duration_sec,
                         upload_batch)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('agent_name', ''),
                        login_id,
                        citrix_uid,
                        acd_id,
                        event_date,
                        login_dt,
                        logout_dt,
                        logout_reason,
                        duration,
                        batch_id
                    ))
                    inserted += 1

                conn.commit()

            return {
                "success": True,
                "rows_processed": inserted,
                "unknown_agents": list(unknown_logins)[:10],
                "warnings": errors if errors else None
            }

        except Exception as e:
            return {"success": False, "error": str(e)}