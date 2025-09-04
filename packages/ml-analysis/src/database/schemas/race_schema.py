"""
競馬レースデータのスキーマ定義
src/database/schemas/race_schema.py
"""

from dataclasses import dataclass
from datetime import date, time, datetime
from typing import Optional, List, Dict
from decimal import Decimal
import re

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
    corner_positions: Optional[Dict[str, str]] = None  # JSONB用
    lap_data: Optional[Dict[str, str]] = None         # JSONB用

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
            'race_conditions': self.race_conditions,
            'corner_positions': self.corner_positions,
            'lap_data': self.lap_data
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

@dataclass
class RacePayout:
    """払い戻し情報"""
    race_id: str
    bet_type: str  # 券種（単勝、複勝、枠連、馬連、ワイド、馬単、三連複、三連単）
    combination: str  # 組み合わせ（"1", "1-3", "1→3→11"など）
    payout_amount: Decimal  # 払い戻し金額

    # Optional fields
    popularity: Optional[int] = None  # 人気順位
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """PostgreSQL挿入用の辞書に変換"""
        return {
            'race_id': self.race_id,
            'bet_type': self.bet_type,
            'combination': self.combination,
            'payout_amount': float(self.payout_amount),
            'popularity': self.popularity
        }


class RaceDataValidator:
    """レースデータのバリデーションクラス"""

    @staticmethod
    def validate_race(race: Race) -> List[str]:
        """レースデータの妥当性チェック"""
        errors = []

        if not race.race_id or len(race.race_id) != 12:
            errors.append("race_id must be 12 characters")

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
    
    @staticmethod
    def validate_race_payout(payout: RacePayout) -> List[str]:
        """払い戻しデータの妥当性チェック"""
        errors = []

        # レースIDの検証
        if not payout.race_id or len(payout.race_id) != 12:
            errors.append("race_id must be 12 characters")

        # 券種の検証
        valid_bet_types = {'単勝', '複勝', '枠連', '枠単', '馬連', 'ワイド', '馬単', '三連複', '三連単'}
        if payout.bet_type not in valid_bet_types:
            errors.append(f"Invalid bet_type: {payout.bet_type}. Must be one of {valid_bet_types}")

        # 組み合わせの検証
        if not payout.combination:
            errors.append("combination is required")
        elif not payout.combination.strip():
            errors.append("combination cannot be empty")

        # 払い戻し金額の検証
        if payout.payout_amount is None:
            errors.append("payout_amount is required")
        elif payout.payout_amount <= 0:
            errors.append("payout_amount must be positive")
        elif payout.payout_amount > Decimal('999999999.99'):  # 10億円未満
            errors.append("payout_amount is too large")

        # 人気の検証（None許可、0は無効）
        if payout.popularity is not None:
            if payout.popularity <= 0:  # 0以下は無効
                errors.append("popularity must be 1 or greater (or None for unavailable data)")
            elif payout.popularity > 99999:  # 現実的な上限
                errors.append("popularity is too large")
        # payout.popularity == None の場合は何もしない（有効）

        # 組み合わせ形式の検証（券種別）
        combination_errors = RaceDataValidator._validate_combination_format(
            payout.bet_type, payout.combination
        )
        errors.extend(combination_errors)

        return errors

    @staticmethod
    def _validate_combination_format(bet_type: str, combination: str) -> List[str]:
        """券種別の組み合わせ形式を検証"""
        errors = []
        
        try:
            # 基本的な文字チェック（数字、ハイフン、矢印のみ許可）
            if not re.match(r'^[0-9\-→\s]+$', combination):
                errors.append(f"combination contains invalid characters: {combination}")
                return errors

            # 券種別の形式チェック
            if bet_type == '単勝':
                # 単一の数字のみ
                if not re.match(r'^\d+$', combination.strip()):
                    errors.append("単勝 combination must be a single number")
                    
            elif bet_type == '複勝':
                # 単一の数字のみ
                if not re.match(r'^\d+$', combination.strip()):
                    errors.append("複勝 combination must be a single number")
                    
            elif bet_type == '枠連':
                # "1 - 2" または "1-2" 形式
                if not re.match(r'^\d+\s*-\s*\d+$', combination):
                    errors.append("枠連 combination must be in format '1 - 2'")
            
            elif bet_type == '枠単':  # 新規追加
                if not re.match(r'^\d+\s*→\s*\d+$', combination):
                    errors.append("枠単 combination must be in format '1 → 2'")
                    
            elif bet_type == '馬連':
                # "1 - 2" または "1-2" 形式
                if not re.match(r'^\d+\s*-\s*\d+$', combination):
                    errors.append("馬連 combination must be in format '1 - 2'")
                    
            elif bet_type == 'ワイド':
                # "1 - 2" または "1-2" 形式
                if not re.match(r'^\d+\s*-\s*\d+$', combination):
                    errors.append("ワイド combination must be in format '1 - 2'")
                    
            elif bet_type == '馬単':
                # "1 → 2" 形式
                if not re.match(r'^\d+\s*→\s*\d+$', combination):
                    errors.append("馬単 combination must be in format '1 → 2'")
                    
            elif bet_type == '三連複':
                # "1 - 2 - 3" 形式
                if not re.match(r'^\d+\s*-\s*\d+\s*-\s*\d+$', combination):
                    errors.append("三連複 combination must be in format '1 - 2 - 3'")
                    
            elif bet_type == '三連単':
                # "1 → 2 → 3" 形式
                if not re.match(r'^\d+\s*→\s*\d+\s*→\s*\d+$', combination):
                    errors.append("三連単 combination must be in format '1 → 2 → 3'")

            # 馬番の範囲チェック（1-18が一般的）
            numbers = re.findall(r'\d+', combination)
            for num_str in numbers:
                num = int(num_str)
                if num < 1 or num > 18:
                    errors.append(f"Horse number {num} is out of valid range (1-18)")

        except Exception as e:
            errors.append(f"Error validating combination format: {str(e)}")

        return errors

    @staticmethod
    def validate_payout_consistency(payouts: List[RacePayout]) -> List[str]:
        """払い戻しデータ群の整合性チェック"""
        errors = []
        
        try:
            # 同じレースの払い戻しデータかチェック
            race_ids = {payout.race_id for payout in payouts}
            if len(race_ids) > 1:
                errors.append(f"Mixed race_ids in payout data: {race_ids}")

            # 券種・組み合わせの重複チェック
            combinations_seen = set()
            for payout in payouts:
                key = (payout.bet_type, payout.combination)
                if key in combinations_seen:
                    errors.append(f"Duplicate payout: {payout.bet_type} {payout.combination}")
                combinations_seen.add(key)

            # 基本的な券種が存在するかチェック（警告レベル）
            bet_types_found = {payout.bet_type for payout in payouts}
            expected_basic_types = {'単勝', '複勝'}
            missing_basic = expected_basic_types - bet_types_found
            if missing_basic:
                errors.append(f"Warning: Missing basic bet types: {missing_basic}")

        except Exception as e:
            errors.append(f"Error validating payout consistency: {str(e)}")

        return errors
