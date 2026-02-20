# modules/normalization.py
import re
from database.db_manager import DatabaseManager

class ShiftNormalizer:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self._load_dictionary()

    def _load_dictionary(self):
        with self.db.connect() as conn:
            cur = conn.execute("SELECT raw_pattern, normalized_shift FROM shift_dictionary WHERE is_active=1")
            self.dict = dict(cur.fetchall())

    def normalize(self, raw_shift):
        if not raw_shift or str(raw_shift).strip().upper() == "OFF":
            return "OFF"
        cleaned = str(raw_shift).strip().replace('"', '').replace("'", "")
        cleaned = re.sub(r'[“”]', '', cleaned)
        cleaned = re.sub(r'(\d)[;,.](\d)', r'\1:\2', cleaned)  # 9,00 -> 9:00
        # Check dictionary
        if cleaned in self.dict:
            return self.dict[cleaned]
        # Try parsing time
        try:
            # handle formats like "9:00" or "09:00"
            parts = cleaned.split(':')
            if len(parts) == 2:
                hour = int(parts[0])
                minute = int(parts[1])
                return f"{hour:02d}:{minute:02d}"
        except:
            pass
        return "UNKNOWN"

    def add_pattern(self, raw, normalized, shift_type="Regular"):
        with self.db.connect() as conn:
            try:
                conn.execute(
                    "INSERT INTO shift_dictionary (raw_pattern, normalized_shift, shift_type) VALUES (?, ?, ?)",
                    (raw, normalized, shift_type)
                )
                conn.commit()
                self._load_dictionary()
                return {"success": True}
            except sqlite3.IntegrityError:
                return {"success": False, "error": "Pattern already exists"}