from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Dict, Any


@dataclass
class Breeder:
    """生産者情報"""
    breeder_id: str
    name_ja: str
    
    # Optional basic info
    name_en: Optional[str] = None
    established_date: Optional[date] = None  # 設立日・開始日
    location: Optional[str] = None  # 所在地（都道府県）
    breeder_type: Optional[str] = None  # individual, corporation, farm
    status: Optional[str] = "active"  # active, retired, closed
    
    # 成績統計（生産した馬の成績）
    total_races: Optional[int] = 0
    wins: Optional[int] = 0
    seconds: Optional[int] = 0
    thirds: Optional[int] = 0
    win_rate: Optional[Decimal] = Decimal('0.0')
    second_rate: Optional[Decimal] = Decimal('0.0')  # 連対率
    show_rate: Optional[Decimal] = Decimal('0.0')  # 3着内率
    total_prize_money: Optional[Decimal] = Decimal('0.0')
    
    # 生産者特有の統計
    total_horses_produced: Optional[int] = 0  # 総生産頭数
    active_horses: Optional[int] = 0  # 現役馬数
    retired_horses: Optional[int] = 0  # 引退馬数
    stakes_wins: Optional[int] = 0  # 重賞勝利数
    grade1_wins: Optional[int] = 0  # G1勝利数
    debut_horses: Optional[int] = 0  # デビュー馬数
    
    # JSONB統計データ
    yearly_stats: Optional[Dict[str, Any]] = None
    race_stats: Optional[Dict[str, Any]] = None  # 重賞・特別・平場別
    track_stats: Optional[Dict[str, Any]] = None  # 芝・ダート別
    distance_stats: Optional[Dict[str, Any]] = None
    produced_horses: Optional[Dict[str, Any]] = None  # 生産馬一覧
    stallion_stats: Optional[Dict[str, Any]] = None  # 種牡馬別成績
    
    # メタデータ
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def calculate_win_rate(self) -> Optional[Decimal]:
        """勝率計算"""
        if self.total_races and self.total_races > 0:
            return Decimal(str(round((self.wins / self.total_races) * 100, 2)))
        return Decimal('0.0')
    
    def calculate_second_rate(self) -> Optional[Decimal]:
        """連対率計算"""
        if self.total_races and self.total_races > 0:
            second_count = (self.wins or 0) + (self.seconds or 0)
            return Decimal(str(round((second_count / self.total_races) * 100, 2)))
        return Decimal('0.0')
    
    def calculate_show_rate(self) -> Optional[Decimal]:
        """3着内率計算"""
        if self.total_races and self.total_races > 0:
            show_count = (self.wins or 0) + (self.seconds or 0) + (self.thirds or 0)
            return Decimal(str(round((show_count / self.total_races) * 100, 2)))
        return Decimal('0.0')
    
    def calculate_debut_rate(self) -> Optional[Decimal]:
        """デビュー率（生産馬のうちデビューした割合）"""
        if self.total_horses_produced and self.total_horses_produced > 0:
            return Decimal(str(round((self.debut_horses / self.total_horses_produced) * 100, 2)))
        return Decimal('0.0')
    
    def calculate_stakes_rate(self) -> Optional[Decimal]:
        """重賞勝利率（デビュー馬あたりの重賞勝利数）"""
        if self.debut_horses and self.debut_horses > 0:
            return Decimal(str(round((self.stakes_wins / self.debut_horses) * 100, 2)))
        return Decimal('0.0')
    
    def update_stats(self):
        """統計情報を自動更新"""
        self.win_rate = self.calculate_win_rate()
        self.second_rate = self.calculate_second_rate()
        self.show_rate = self.calculate_show_rate()
    
    def to_dict(self) -> dict:
        """PostgreSQL挿入用の辞書に変換"""
        return {
            'breeder_id': self.breeder_id,
            'name_ja': self.name_ja,
            'name_en': self.name_en,
            'established_date': self.established_date.isoformat() if self.established_date else None,
            'location': self.location,
            'breeder_type': self.breeder_type,
            'status': self.status,
            'total_races': self.total_races,
            'wins': self.wins,
            'seconds': self.seconds,
            'thirds': self.thirds,
            'win_rate': float(self.win_rate) if self.win_rate else None,
            'second_rate': float(self.second_rate) if self.second_rate else None,
            'show_rate': float(self.show_rate) if self.show_rate else None,
            'total_prize_money': float(self.total_prize_money) if self.total_prize_money else None,
            'total_horses_produced': self.total_horses_produced,
            'active_horses': self.active_horses,
            'retired_horses': self.retired_horses,
            'stakes_wins': self.stakes_wins,
            'grade1_wins': self.grade1_wins,
            'debut_horses': self.debut_horses,
            'yearly_stats': self.yearly_stats,
            'race_stats': self.race_stats,
            'track_stats': self.track_stats,
            'distance_stats': self.distance_stats,
            'produced_horses': self.produced_horses,
            'stallion_stats': self.stallion_stats
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Breeder':
        """辞書からBreederオブジェクトを作成"""
        # 日付フィールドの変換
        established_date = None
        if data.get('established_date'):
            if isinstance(data['established_date'], str):
                established_date = date.fromisoformat(data['established_date'])
            else:
                established_date = data['established_date']
        
        # Decimalフィールドの変換
        win_rate = Decimal(str(data['win_rate'])) if data.get('win_rate') else Decimal('0.0')
        second_rate = Decimal(str(data['second_rate'])) if data.get('second_rate') else Decimal('0.0')
        show_rate = Decimal(str(data['show_rate'])) if data.get('show_rate') else Decimal('0.0')
        total_prize_money = Decimal(str(data['total_prize_money'])) if data.get('total_prize_money') else Decimal('0.0')
        
        return cls(
            breeder_id=data['breeder_id'],
            name_ja=data['name_ja'],
            name_en=data.get('name_en'),
            established_date=established_date,
            location=data.get('location'),
            breeder_type=data.get('breeder_type'),
            status=data.get('status', 'active'),
            total_races=data.get('total_races', 0),
            wins=data.get('wins', 0),
            seconds=data.get('seconds', 0),
            thirds=data.get('thirds', 0),
            win_rate=win_rate,
            second_rate=second_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            total_horses_produced=data.get('total_horses_produced', 0),
            active_horses=data.get('active_horses', 0),
            retired_horses=data.get('retired_horses', 0),
            stakes_wins=data.get('stakes_wins', 0),
            grade1_wins=data.get('grade1_wins', 0),
            debut_horses=data.get('debut_horses', 0),
            yearly_stats=data.get('yearly_stats'),
            race_stats=data.get('race_stats'),
            track_stats=data.get('track_stats'),
            distance_stats=data.get('distance_stats'),
            produced_horses=data.get('produced_horses'),
            stallion_stats=data.get('stallion_stats'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )


@dataclass
class BreederPerformance:
    """生産者の特定条件下での成績"""
    breeder_id: str
    condition: str  # "turf", "dirt", "short_distance", "2023", "grade", "stallion_name", etc.
    races: int = 0
    wins: int = 0
    seconds: int = 0
    thirds: int = 0
    win_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    second_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    show_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    prize_money: Decimal = field(default_factory=lambda: Decimal('0.0'))
    horses_produced: int = 0  # 該当条件での生産頭数
    
    def calculate_rates(self):
        """勝率・連対率・3着内率を計算"""
        if self.races > 0:
            self.win_rate = Decimal(str(round((self.wins / self.races) * 100, 2)))
            second_count = self.wins + self.seconds
            self.second_rate = Decimal(str(round((second_count / self.races) * 100, 2)))
            show_count = self.wins + self.seconds + self.thirds
            self.show_rate = Decimal(str(round((show_count / self.races) * 100, 2)))
    
    def to_dict(self) -> dict:
        """辞書形式に変換"""
        return {
            'races': self.races,
            'wins': self.wins,
            'seconds': self.seconds,
            'thirds': self.thirds,
            'win_rate': float(self.win_rate),
            'second_rate': float(self.second_rate),
            'show_rate': float(self.show_rate),
            'prize_money': float(self.prize_money),
            'horses_produced': self.horses_produced
        }