# modules/attendance_engine.py
import pandas as pd
from datetime import datetime, timedelta

class AttendanceEngine:
    def __init__(self, db, normalizer, audit):
        self.db = db
        self.normalizer = normalizer
        self.audit = audit

    def calculate_for_date(self, calc_date):
        year_month = f"{calc_date.year}_{calc_date.month:02d}"
        with self.db.connect() as conn:
            # Get live roster for that date
            roster_df = pd.read_sql_query(f"""
                SELECT r.citrix_uid, r.acd_id, r.scheduled_shift as updated_shift,
                       a.name, a.queue, a.status as hc_status
                FROM roster_live_{year_month} r
                LEFT JOIN agents_master a ON r.citrix_uid = a.citrix_uid
                WHERE r.shift_date = ?
            """, conn, params=(calc_date,))

            # Get CMS data (if available)
            cms_df = pd.read_sql_query(f"""
                SELECT citrix_uid, staffed_time_sec, ans_calls, handle_time_sec
                FROM cms_raw_{year_month}
                WHERE report_date = ?
            """, conn, params=(calc_date,))

            # Get Aspect/EIM sessions (aggregated)
            aspect_df = pd.read_sql_query(f"""
                SELECT citrix_uid, SUM(session_duration_sec) as total_staff_sec
                FROM aspect_raw_{year_month}
                WHERE event_date = ?
                GROUP BY citrix_uid
            """, conn, params=(calc_date,))

            eim_df = pd.read_sql_query(f"""
                SELECT citrix_uid, SUM(session_duration_sec) as total_staff_sec
                FROM eim_raw_{year_month}
                WHERE event_date = ?
                GROUP BY citrix_uid
            """, conn, params=(calc_date,))

            # Combine staff time from best source
            # Priority: EIM > Aspect > CMS
            staff_time = {}
            for _, row in eim_df.iterrows():
                staff_time[row['citrix_uid']] = row['total_staff_sec']
            for _, row in aspect_df.iterrows():
                if row['citrix_uid'] not in staff_time:
                    staff_time[row['citrix_uid']] = row['total_staff_sec']
            for _, row in cms_df.iterrows():
                if row['citrix_uid'] not in staff_time:
                    staff_time[row['citrix_uid']] = row['staffed_time_sec']

            # Process each agent
            attendance_records = []
            for _, roster_row in roster_df.iterrows():
                citrix = roster_row['citrix_uid']
                scheduled = roster_row['updated_shift']
                staff_sec = staff_time.get(citrix, 0)
                staff_min = staff_sec / 60.0

                # Determine attendance status
                if scheduled == "OFF":
                    status = "Scheduled Off"
                    final = "OFF"
                    reason = ""
                elif staff_sec == 0:
                    status = "Absent"
                    final = "Absent"
                    reason = "No Show"
                else:
                    # Parse scheduled duration (simplified)
                    # In reality you'd have shift start/end times, but we'll use a heuristic
                    if ":" in scheduled:
                        try:
                            # assume 9h shift if not OFF
                            worked_hours = staff_sec / 3600.0
                            if worked_hours >= 9 - 0.5:  # 30 min tolerance
                                status = "Full Shift"
                                final = "Full Shift"
                                reason = "OK"
                            elif 4 <= worked_hours < 4.5:
                                status = "Half Day"
                                final = "Half Day Annual"
                                reason = "Left Early (4h)"
                            elif worked_hours >= 10:
                                status = "Overtime"
                                final = "Overtime"
                                reason = ">10h"
                            elif worked_hours < 4.5:
                                status = "Absent"
                                final = "Absent"
                                reason = "<4.5h"
                            else:
                                status = "Partial"
                                final = "Partial"
                                reason = "Other"
                        except:
                            status = "Unknown"
                            final = scheduled
                            reason = "Shift parse error"
                    else:
                        status = "Unknown"
                        final = scheduled
                        reason = "Non‑time shift"

                # Determine data source used
                if citrix in eim_df['citrix_uid'].values:
                    source = 'EIM'
                elif citrix in aspect_df['citrix_uid'].values:
                    source = 'Aspect'
                elif citrix in cms_df['citrix_uid'].values:
                    source = 'CMS'
                else:
                    source = 'None'

                attendance_records.append((
                    citrix,
                    roster_row.get('acd_id'),
                    calc_date,
                    roster_row.get('scheduled_shift'),  # original? need original from roster_original
                    scheduled,
                    staff_sec,
                    staff_min,
                    status,
                    final,
                    reason,
                    roster_row.get('hc_status'),
                    source,
                    100 if source != 'None' else 0,
                    ''
                ))

            # Clear previous records for this date
            conn.execute(f"DELETE FROM attendance_processed_{year_month} WHERE shift_date=?", (calc_date,))
            # Insert new
            conn.executemany(f"""
                INSERT INTO attendance_processed_{year_month}
                (citrix_uid, acd_id, shift_date, original_shift, updated_shift,
                 staff_time_sec, staff_time_min, staff_time_validation,
                 attendance_status, final_shift, absenteeism_reason,
                 hc_status, data_source, confidence_score, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, attendance_records)
            conn.commit()
            return {"success": True, "processed": len(attendance_records)}