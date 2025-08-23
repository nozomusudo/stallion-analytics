# src/scraping/utils/constants.py

# 日本語→英語フィールドマッピング
HORSE_FIELD_MAPPING = {
    # 基本情報
    '馬名': 'name_ja',
    '英字名': 'name_en', 
    '生年月日': 'birth_date',
    '調教師': 'trainer',
    '馬主': 'owner',
    '生産者': 'breeder',
    '産地': 'birthplace',
    
    # 血統情報
    '父': 'sire',
    '母': 'dam', 
    '母父': 'maternal_grandsire',
    
    # 体格情報
    '馬体重': 'weight',
    '体高': 'height',
    
    # 成績情報
    '通算成績': 'career_record',
    '獲得賞金 (中央)': 'total_prize_central',
    '獲得賞金 (地方)': 'total_prize_local',
    '重賞勝利': 'graded_wins',
    
    # 主な勝ち鞍
    '主な勝ち鞍': 'main_victories',
    
    # 募集情報
    '募集情報': 'offering_info',
    
    # その他
    'セリ取引価格': 'auction_price',
    '近親馬': 'related_horses'
}

# 血統関係タイプ
RELATION_TYPES = {
    'sire_of': '父子関係',
    'dam_of': '母子関係', 
    'mating': '種付関係',
    'bms_of': 'BMS関係'
}

# 性別マッピング
SEX_MAPPING = {
    '牡': 'stallion',
    '牝': 'mare',
    'せん': 'gelding'
}

# レースグレード
RACE_GRADES = ['G1', 'G2', 'G3', 'OP', 'L']