# modules/swap_workflow.py
from datetime import datetime, time
import pandas as pd

class SwapManager:
    def __init__(self, db, normalizer, audit):
        self.db = db
        self.normalizer = normalizer
        self.audit = audit

    def create_request(self, requester, agent_a_acd, agent_b_acd, shift_date,
                       new_shift_a, new_shift_b=None, leave_type=None):
        # Validate
        if shift_date == datetime.now().date():
            return {"success": False, "error": "Same-day swaps not allowed"}
        if datetime.now().time() > time(23,59,59) and shift_date == datetime.now().date() + timedelta(days=1):
            return {"success": False, "error": "Swap deadline (23:59) passed for tomorrow"}

        with self.db.connect() as conn:
            # Get agent info
            a = conn.execute("SELECT citrix_uid, name FROM agents_master WHERE acd_id=?", (agent_a_acd,)).fetchone()
            if not a:
                return {"success": False, "error": "Agent A not found"}
            a_citrix = a['citrix_uid']

            # Get current shift for agent A
            year_month = f"{shift_date.year}_{shift_date.month:02d}"
            cur_shift_a = conn.execute(f"""
                SELECT scheduled_shift FROM roster_live_{year_month}
                WHERE citrix_uid=? AND shift_date=?
            """, (a_citrix, shift_date)).fetchone()
            if not cur_shift_a:
                return {"success": False, "error": "No shift found for agent A on that date"}

            original_a = cur_shift_a['scheduled_shift']

            # If swap with B
            b_citrix = None
            original_b = None
            if agent_b_acd:
                b = conn.execute("SELECT citrix_uid, name FROM agents_master WHERE acd_id=?", (agent_b_acd,)).fetchone()
                if not b:
                    return {"success": False, "error": "Agent B not found"}
                b_citrix = b['citrix_uid']
                cur_shift_b = conn.execute(f"""
                    SELECT scheduled_shift FROM roster_live_{year_month}
                    WHERE citrix_uid=? AND shift_date=?
                """, (b_citrix, shift_date)).fetchone()
                if not cur_shift_b:
                    return {"success": False, "error": "No shift found for agent B on that date"}
                original_b = cur_shift_b['scheduled_shift']

            # Insert swap request
            conn.execute("""
                INSERT INTO shift_swaps
                (requester_citrix, agent_a_citrix, agent_b_citrix, shift_date,
                 original_shift_a, original_shift_b, requested_shift_a, requested_shift_b,
                 swap_type, leave_type, submitted_by, submitted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                requester,
                a_citrix,
                b_citrix,
                shift_date,
                original_a,
                original_b,
                new_shift_a,
                new_shift_b,
                'Swap' if agent_b_acd else 'Update',
                leave_type,
                requester
            ))
            conn.commit()
            swap_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return {"success": True, "swap_id": swap_id}

    def get_pending_swaps(self):
        with self.db.connect() as conn:
            df = pd.read_sql_query("""
                SELECT s.*, a1.name as agent_a_name, a2.name as agent_b_name
                FROM shift_swaps s
                LEFT JOIN agents_master a1 ON s.agent_a_citrix = a1.citrix_uid
                LEFT JOIN agents_master a2 ON s.agent_b_citrix = a2.citrix_uid
                WHERE s.status = 'Pending'
                ORDER BY s.submitted_at DESC
            """, conn)
            return df

    def approve_swap(self, swap_id, reviewer):
        with self.db.connect() as conn:
            # Get swap details
            swap = conn.execute("SELECT * FROM shift_swaps WHERE swap_id=?", (swap_id,)).fetchone()
            if not swap:
                return {"success": False, "error": "Swap not found"}
            if swap['status'] != 'Pending':
                return {"success": False, "error": "Swap already processed"}

            year_month = f"{pd.to_datetime(swap['shift_date']).year}_{pd.to_datetime(swap['shift_date']).month:02d}"
            # Update roster_live for agent A
            conn.execute(f"""
                UPDATE roster_live_{year_month}
                SET scheduled_shift=?, shift_source='Swap', modified_by=?, modified_at=CURRENT_TIMESTAMP, approved_by=?, approved_at=CURRENT_TIMESTAMP
                WHERE citrix_uid=? AND shift_date=?
            """, (swap['requested_shift_a'], reviewer, reviewer, swap['agent_a_citrix'], swap['shift_date']))
            # If agent B exists
            if swap['agent_b_citrix']:
                conn.execute(f"""
                    UPDATE roster_live_{year_month}
                    SET scheduled_shift=?, shift_source='Swap', modified_by=?, modified_at=CURRENT_TIMESTAMP, approved_by=?, approved_at=CURRENT_TIMESTAMP
                    WHERE citrix_uid=? AND shift_date=?
                """, (swap['requested_shift_b'], reviewer, reviewer, swap['agent_b_citrix'], swap['shift_date']))
            # Update swap status
            conn.execute("""
                UPDATE shift_swaps
                SET status='Approved', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP
                WHERE swap_id=?
            """, (reviewer, swap_id))
            conn.commit()
            return {"success": True}