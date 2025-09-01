# src/scraping/extractors/horse/pedigree_extractor.py

import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

class PedigreeExtractor:
    """血統情報を抽出するクラス"""
    
    def extract_pedigree_ids(self, soup: BeautifulSoup) -> Dict:
        """血統テーブルから直接カラム用のIDを抽出"""
        try:
            pedigree_ids = {}
            
            blood_table = soup.find('table', class_='blood_table')
            if not blood_table:
                return pedigree_ids
            
            rows = blood_table.find_all('tr')
            if len(rows) >= 4:
                # 1行目: 父
                sire_link = rows[0].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if sire_link:
                    href = sire_link.get('href', '')
                    sire_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if sire_id_match:
                        pedigree_ids['sire_id'] = sire_id_match.group(1)
                        # print(f"    🧬 父ID取得: {pedigree_ids['sire_id']}")
                
                # 3行目: 母
                dam_link = rows[2].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if dam_link:
                    href = dam_link.get('href', '')
                    dam_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if dam_id_match:
                        pedigree_ids['dam_id'] = dam_id_match.group(1)
                        # print(f"    🧬 母ID取得: {pedigree_ids['dam_id']}")
                
                # 4行目: 母父
                bms_link = rows[3].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if bms_link:
                    href = bms_link.get('href', '')
                    bms_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if bms_id_match:
                        pedigree_ids['maternal_grandsire_id'] = bms_id_match.group(1)
                        # print(f"    🧬 母父ID取得: {pedigree_ids['maternal_grandsire_id']}")
            
            return pedigree_ids
            
        except Exception as e:
            print(f"      ❌ 血統ID抽出エラー: {e}")
            return {}
    
    def extract_relations_from_url(self, horse_id: str, session) -> List[Dict]:
        """血統関係ページから関係を取得"""
        pedigree_url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
        relations = []
        
        try:
            # print(f"  🌳 血統関係取得: {horse_id}")
            response = session.get(pedigree_url, timeout=15)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            pedigree_table = self.find_pedigree_table(soup)
            
            if pedigree_table:
                relations.extend(self.extract_direct_relations(pedigree_table, horse_id))
            
            return relations
            
        except Exception as e:
            print(f"    ❌ 血統関係取得エラー: {e}")
            return []
    
    def find_pedigree_table(self, soup: BeautifulSoup):
        """血統表を探す"""
        table_candidates = [
            soup.find('table', class_='blood_table_detail'),
            soup.find('table', summary='5代血統表'),
            soup.find('table', class_='blood_table')
        ]
        
        for table in table_candidates:
            if table:
                return table
        
        return None
    
    def extract_direct_relations(self, table, horse_id: str) -> List[Dict]:
        """直接的な血統関係（父・母・母父）を抽出"""
        relations = []
        
        try:
            rows = table.find_all('tr')
            
            for row_index, row in enumerate(rows):
                cells = row.find_all('td')
                
                for cell in cells:
                    horse_link = cell.find('a', href=re.compile(r'/horse/[0-9a-zA-Z]+/'))
                    
                    if horse_link:
                        href = horse_link.get('href', '')
                        horse_id_match = re.search(r'/horse/([0-9a-zA-Z]+)/', href)
                        
                        if horse_id_match:
                            related_horse_id = horse_id_match.group(1)
                            horse_name = horse_link.get_text(strip=True)
                            
                            rowspan = cell.get('rowspan', '1')
                            relation_type = self.determine_relation_type(row_index, rowspan)
                            
                            if relation_type:
                                relations.append({
                                    'horse_a_id': related_horse_id,
                                    'horse_b_id': horse_id,
                                    'relation_type': relation_type,
                                    'children_ids': None
                                })
            
        except Exception as e:
            print(f"      ❌ 関係抽出エラー: {e}")
        
        return relations
    
    def determine_relation_type(self, row_index: int, rowspan: str) -> Optional[str]:
        """血統表の位置から関係タイプを判定"""
        try:
            if rowspan == '16':  # 1世代目
                if row_index == 0:
                    return 'sire_of'  # 父
                elif row_index >= 16:
                    return 'dam_of'   # 母
            elif rowspan == '8':  # 2世代目
                if row_index == 16:  # 母父の位置
                    return 'bms_of'
        except:
            pass
        
        return None
    
    def extract_pedigree_ids_from_relations(self, relations: List[Dict], horse_id: str) -> Dict:
        """血統関係から直接カラム用のIDを抽出"""
        pedigree_ids = {}
        
        for relation in relations:
            if relation['horse_b_id'] == horse_id:
                if relation['relation_type'] == 'sire_of':
                    pedigree_ids['sire_id'] = relation['horse_a_id']
                    # print(f"    🧬 父ID設定: {pedigree_ids['sire_id']}")
                elif relation['relation_type'] == 'dam_of':
                    pedigree_ids['dam_id'] = relation['horse_a_id']
                    # print(f"    🧬 母ID設定: {pedigree_ids['dam_id']}")
                elif relation['relation_type'] == 'bms_of':
                    pedigree_ids['maternal_grandsire_id'] = relation['horse_a_id']
                    # print(f"    🧬 母父ID設定: {pedigree_ids['maternal_grandsire_id']}")
        
        return pedigree_ids
    
    def create_mating_relations(self, pedigree_ids: Dict, horse_id: str, storage) -> List[Dict]:
        """父と母の種付関係を作成・更新（children_ids付き）"""
        mating_relations = []
        
        sire_id = pedigree_ids.get('sire_id')
        dam_id = pedigree_ids.get('dam_id')
        
        if sire_id and dam_id and storage.supabase:
            # 既存のmating関係をチェック
            existing_mating = self.find_existing_mating(sire_id, dam_id, storage.supabase)
            
            if existing_mating:
                # 既存関係に子供IDを追加
                updated_relation = self.add_child_to_mating(existing_mating, horse_id, storage.supabase)
                if updated_relation:
                    # print(f"    💕 種付関係更新: {sire_id} × {dam_id} → 子供追加: {horse_id}")
                    mating_relations.append(updated_relation)
            else:
                # 新規mating関係を作成
                new_relation = {
                    'horse_a_id': sire_id,  # 父
                    'horse_b_id': dam_id,   # 母
                    'relation_type': 'mating',
                    'children_ids': [horse_id]  # 最初の子供として追加
                }
                mating_relations.append(new_relation)
                # print(f"    💕 種付関係新規作成: {sire_id} × {dam_id} → 子供: {horse_id}")
        
        return mating_relations
    
    def find_existing_mating(self, sire_id: str, dam_id: str, supabase):
        """既存のmating関係を検索"""
        try:
            # 父→母 または 母→父 の両方向で検索
            result1 = supabase.table('horse_relations').select('*').eq(
                'horse_a_id', sire_id
            ).eq('horse_b_id', dam_id).eq(
                'relation_type', 'mating'
            ).execute()
            
            if result1.data:
                return result1.data[0]
            
            # 逆方向も確認
            result2 = supabase.table('horse_relations').select('*').eq(
                'horse_a_id', dam_id
            ).eq('horse_b_id', sire_id).eq(
                'relation_type', 'mating'
            ).execute()
            
            if result2.data:
                return result2.data[0]
            
            return None
            
        except Exception as e:
            print(f"      ❌ 既存mating検索エラー: {e}")
            return None
    
    def add_child_to_mating(self, existing_mating: dict, horse_id: str, supabase):
        """既存のmating関係に子供IDを追加"""
        try:
            # children_ids が None の場合は空リストで初期化
            current_children = existing_mating.get('children_ids') or []
            
            # リストでない場合も空リストで初期化
            if not isinstance(current_children, list):
                current_children = []
            
            # 既に子供リストに含まれていない場合のみ追加
            if horse_id not in current_children:
                updated_children = current_children + [horse_id]
                
                # データベースを更新
                update_result = supabase.table('horse_relations').update({
                    'children_ids': updated_children
                }).eq('id', existing_mating['id']).execute()
                
                if update_result.data:
                    updated_relation = update_result.data[0]
                    # print(f"      👶 子供ID追加完了: 現在の子供数 {len(updated_children)}")
                    return updated_relation
            else:
                # print(f"      💡 子供ID既存: {horse_id} は既にリストに含まれています")
                return existing_mating
                
        except Exception as e:
            print(f"      ❌ 子供ID追加エラー: {e}")
            return None