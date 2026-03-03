import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

class UTPNotesManager:
    """Manages UTP configuration notes with JSON persistence."""
    
    def __init__(self, file_path: Optional[Path] = None):
        if file_path is None:
            # Default path relative to project root
            self.file_path = Path("data/utp_notes.json")
        else:
            self.file_path = file_path
            
        # Ensure the directory exists
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize file if it doesn't exist
        if not self.file_path.exists():
            self._save_to_disk([])

    def _load_from_disk(self) -> List[Dict]:
        """Loads notes from the JSON file."""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _save_to_disk(self, notes: List[Dict]):
        """Saves notes to the JSON file."""
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(notes, f, indent=2, ensure_ascii=False)

    def get_all_notes(self) -> List[Dict]:
        """Returns all stored notes."""
        return self._load_from_disk()

    def add_note(self, title: str, text: str, city: str, utp_id: str) -> Dict:
        """Adds a new note and saves to disk."""
        notes = self._load_from_disk()
        
        new_note = {
            "id": str(uuid.uuid4()),
            "title": title,
            "text": text,
            "city": city,
            "utp_id": utp_id,
            "timestamp": datetime.now().isoformat()
        }
        
        notes.append(new_note)
        self._save_to_disk(notes)
        return new_note

    def delete_note(self, note_id: str) -> bool:
        """Deletes a note by ID and saves to disk."""
        notes = self._load_from_disk()
        initial_count = len(notes)
        notes = [n for n in notes if n["id"] != note_id]
        
        if len(notes) < initial_count:
            self._save_to_disk(notes)
            return True
        return False
