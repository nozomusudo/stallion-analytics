from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Dict, Any


@dataclass
class Owner:
    """馬主情報"""
    owner_id: str
    name_ja: str
    
    # Optional basic info
    name_en: Optional[str] = None
    birthdate: Optional[date] = None
    owner_type: Optional[str] = None  # individual, corporation, partnership, etc.
    license_date: Optional[date] = None  # 馬主免許取得日
    status: Optional[str] = "active"  # active, retired, suspended
    
    # 成績統計
    total_races: Optional[int] = 0
    wins: Optional[int] = 0
    seconds: Optional[int] = 0
    thirds: Optional[int] = 0
    win_rate: Optional[Decimal] = Decimal('0.0')
    second_rate: Optional[Decimal] = Decimal('0.0')  # 連対率
    show_rate: Optional[Decimal] = Decimal('0.0')  # 3着内率
    total_prize_money: Optional[Decimal] = Decimal('0.0')
    
    # 馬主特有の統計
    total_horses: Optional[int] = 0  # 所有馬頭数
    active_horses: Optional[int] = 0  # 現役所有馬数
    retired_horses: Optional[int] = 0  # 引退馬数
    stakes_wins: Optional[int] = 0  # 重賞勝利数
    grade1_wins: Optional[int] = 0  # G1勝利数
    
    # JSONB統計データ
    yearly_stats: Optional[Dict[str, Any]] = None
    race_stats: Optional[Dict[str, Any]] = None  # 重賞・特別・平場別
    track_stats: Optional[Dict[str, Any]] = None  # 芝・ダート別
    distance_stats: Optional[Dict[str, Any]] = None
    horse_list: Optional[Dict[str, Any]] = None  # 所有馬一覧
    
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
    
    def calculate_horse_performance_rate(self) -> Optional[Decimal]:
        """馬あたり平均勝利数"""
        if self.total_horses and self.total_horses > 0:
            return Decimal(str(round((self.wins / self.total_horses), 2)))
        return Decimal('0.0')
    
    def update_stats(self):
        """統計情報を自動更新"""
        self.win_rate = self.calculate_win_rate()
        self.second_rate = self.calculate_second_rate()
        self.show_rate = self.calculate_show_rate()
    
    def to_dict(self) -> dict:
        """PostgreSQL挿入用の辞書に変換"""
        return {
            'owner_id': self.owner_id,
            'name_ja': self.name_ja,
            'name_en': self.name_en,
            'birthdate': self.birthdate.isoformat() if self.birthdate else None,
            'owner_type': self.owner_type,
            'license_date': self.license_date.isoformat() if self.license_date else None,
            'status': self.status,
            'total_races': self.total_races,
            'wins': self.wins,
            'seconds': self.seconds,
            'thirds': self.thirds,
            'win_rate': float(self.win_rate) if self.win_rate else None,
            'second_rate': float(self.second_rate) if self.second_rate else None,
            'show_rate': float(self.show_rate) if self.show_rate else None,
            'total_prize_money': float(self.total_prize_money) if self.total_prize_money else None,
            'total_horses': self.total_horses,
            'active_horses': self.active_horses,
            'retired_horses': self.retired_horses,
            'stakes_wins': self.stakes_wins,
            'grade1_wins': self.grade1_wins,
            'yearly_stats': self.yearly_stats,
            'race_stats': self.race_stats,
            'track_stats': self.track_stats,
            'distance_stats': self.distance_stats,
            'horse_list': self.horse_list
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Owner':
        """辞書からOwnerオブジェクトを作成"""
        # 日付フィールドの変換
        birthdate = None
        if data.get('birthdate'):
            if isinstance(data['birthdate'], str):
                birthdate = date.fromisoformat(data['birthdate'])
            else:
                birthdate = data['birthdate']
        
        license_date = None
        if data.get('license_date'):
            if isinstance(data['license_date'], str):
                license_date = date.fromisoformat(data['license_date'])
            else:
                license_date = data['license_date']
        
        # Decimalフィールドの変換
        win_rate = Decimal(str(data['win_rate'])) if data.get('win_rate') else Decimal('0.0')
        second_rate = Decimal(str(data['second_rate'])) if data.get('second_rate') else Decimal('0.0')
        show_rate = Decimal(str(data['show_rate'])) if data.get('show_rate') else Decimal('0.0')
        total_prize_money = Decimal(str(data['total_prize_money'])) if data.get('total_prize_money') else Decimal('0.0')
        
        return cls(
            owner_id=data['owner_id'],
            name_ja=data['name_ja'],
            name_en=data.get('name_en'),
            birthdate=birthdate,
            owner_type=data.get('owner_type'),
            license_date=license_date,
            status=data.get('status', 'active'),
            total_races=data.get('total_races', 0),
            wins=data.get('wins', 0),
            seconds=data.get('seconds', 0),
            thirds=data.get('thirds', 0),
            win_rate=win_rate,
            second_rate=second_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            total_horses=data.get('total_horses', 0),
            active_horses=data.get('active_horses', 0),
            retired_horses=data.get('retired_horses', 0),
            stakes_wins=data.get('stakes_wins', 0),
            grade1_wins=data.get('grade1_wins', 0),
            yearly_stats=data.get('yearly_stats'),
            race_stats=data.get('race_stats'),
            track_stats=data.get('track_stats'),
            distance_stats=data.get('distance_stats'),
            horse_list=data.get('horse_list'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )


@dataclass
class OwnerPerformance:
    """馬主の特定条件下での成績"""
    owner_id: str
    condition: str  # "turf", "dirt", "short_distance", "2023", "grade", etc.
    races: int = 0
    wins: int = 0
    seconds: int = 0
    thirds: int = 0
    win_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    second_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    show_rate: Decimal = field(default_factory=lambda: Decimal('0.0'))
    prize_money: Decimal = field(default_factory=lambda: Decimal('0.0'))
    horses_used: int = 0  # 使用馬頭数
    
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
            'horses_used': self.horses_used
        }