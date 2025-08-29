"""
競馬レースデータのスキーマ定義
src/database/schemas/race_schema.py
"""

from dataclasses import dataclass
from datetime import date, time, datetime
from typing import Optional, List
from decimal import Decimal

@dataclass
class Race:
    """レース基本情報"""
    race_id: str
    race_date: date
    track_name: str
    race_number: int
    race_name: str
    distance: int
    track_type: str
    total_horses: int

    # Optional fields
    grade: Optional[str] = None
    track_direction: Optional[str] = None
    weather: Optional[str] = None
    track_condition: Optional[str] = None
    start_time: Optional[time] = None
    winning_time: Optional[str] = None
    pace: Optional[str] = None
    prize_1st: Optional[Decimal] = None
    race_class: Optional[str] = None
    race_conditions: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Supabase挿入用の辞書に変換"""
        return {
            'race_id': self.race_id,
            'race_date': self.race_date.isoformat(),
            'track_name': self.track_name,
            'race_number': self.race_number,
            'race_name': self.race_name,
            'grade': self.grade,
            'distance': self.distance,
            'track_type': self.track_type,
            'track_direction': self.track_direction,
            'weather': self.weather,
            'track_condition': self.track_condition,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'total_horses': self.total_horses,
            'winning_time': self.winning_time,
            'pace': self.pace,
            'prize_1st': float(self.prize_1st) if self.prize_1st else None,
            'race_class': self.race_class,
            'race_conditions': self.race_conditions
        }

@dataclass
class RaceResult:
    """レース結果（各馬の成績）"""
    race_id: str
    horse_id: str
    horse_name: str
    bracket_number: int
    horse_number: int
    age: int
    sex: str
    jockey_weight: Decimal
    jockey_name: str
    trainer_name: str

    # Optional fields
    finish_position: Optional[int] = None
    jockey_id: Optional[str] = None
    trainer_region: Optional[str] = None
    trainer_id: Optional[str] = None
    race_time: Optional[str] = None
    time_diff: Optional[str] = None
    passing_order: Optional[str] = None
    last_3f: Optional[Decimal] = None
    odds: Optional[Decimal] = None
    popularity: Optional[int] = None
    horse_weight: Optional[int] = None
    weight_change: Optional[int] = None
    prize_money: Optional[Decimal] = None
    owner_id: Optional[str] = None
    owner_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Supabase挿入用の辞書に変換"""
        return {
            'race_id': self.race_id,
            'horse_id': self.horse_id,
            'horse_name': self.horse_name,
            'finish_position': self.finish_position,
            'bracket_number': self.bracket_number,
            'horse_number': self.horse_number,
            'age': self.age,
            'sex': self.sex,
            'jockey_weight': float(self.jockey_weight),
            'jockey_id': self.jockey_id,
            'jockey_name': self.jockey_name,
            'trainer_region': self.trainer_region,
            'trainer_id': self.trainer_id,
            'trainer_name': self.trainer_name,
            'race_time': self.race_time,
            'time_diff': self.time_diff,
            'passing_order': self.passing_order,
            'last_3f': float(self.last_3f) if self.last_3f else None,
            'odds': float(self.odds) if self.odds else None,
            'popularity': self.popularity,
            'horse_weight': self.horse_weight,
            'weight_change': self.weight_change,
            'prize_money': float(self.prize_money) if self.prize_money else None,
            'owner_id': self.owner_id,
            'owner_name': self.owner_name
        }

class RaceDataValidator:
    """レースデータのバリデーションクラス"""

    @staticmethod
    def validate_race(race: Race) -> List[str]:
        """レースデータの妥当性チェック"""
        errors = []

        if not race.race_id or len(race.race_id) != 12:
            errors.append("race_id must be 12 characters")

        if race.distance <= 0:
            errors.append("distance must be positive")

        return errors

    @staticmethod
    def validate_race_result(result: RaceResult) -> List[str]:
        """レース結果データの妥当性チェック"""
        errors = []

        if not result.race_id or len(result.race_id) != 12:
            errors.append("race_id must be 12 characters")

        if not result.horse_id:
            errors.append("horse_id is required")

        return errors
