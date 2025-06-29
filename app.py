# 必要なライブラリをインポート
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid
import re
import logging
from typing import Optional, List, Dict, Any

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Streamlitアプリの基本設定 ---
st.set_page_config(layout="wide")
st.title('初心者向けSQLジェネレーター')

# --- セキュリティ設定 ---
def sanitize_identifier(identifier: Optional[str]) -> Optional[str]:
    """識別子をサニタイズする"""
    if not identifier or not isinstance(identifier, str):
        return None
    # 許可される文字のみを残す
    sanitized = re.sub(r'[^a-zA-Z0-9_"]', '', identifier)
    return sanitized if sanitized else None

def validate_sql_value(value: Optional[str]) -> bool:
    """SQL値の検証"""
    if not value or not isinstance(value, str):
        return True
    # 危険な文字列パターンをチェック
    dangerous_patterns = [
        r'--',  # SQLコメント
        r'/\*.*\*/',  # SQLコメント
        r';\s*$',  # セミコロン
        r'DROP\s+',  # DROP文
        r'DELETE\s+',  # DELETE文
        r'UPDATE\s+',  # UPDATE文
        r'INSERT\s+',  # INSERT文
        r'CREATE\s+',  # CREATE文
        r'ALTER\s+',  # ALTER文
    ]
    for pattern in dangerous_patterns:
        if re.search(pattern, value, re.IGNORECASE):
            return False
    return True

# --- エラーハンドリング関数 ---
def handle_database_error(operation: str, error: Exception) -> None:
    """データベースエラーの統一処理"""
    error_msg = f"{operation}中にエラーが発生しました: {str(error)}"
    logger.error(error_msg)
    st.error(error_msg)
    
    # ユーザーフレンドリーなエラーメッセージ
    if "connection" in str(error).lower():
        st.info("データベース接続を確認してください。")
    elif "permission" in str(error).lower():
        st.info("必要な権限があるか確認してください。")
    elif "not found" in str(error).lower():
        st.info("指定されたオブジェクトが存在するか確認してください。")

# --- セッションステートの初期化 ---
if 'select_items' not in st.session_state:
    st.session_state.select_items = []
if 'where_conditions' not in st.session_state:
    st.session_state.where_conditions = []
if 'joins' not in st.session_state:
    st.session_state.joins = []

# --- Snowflakeセッションの取得 ---
try:
    session = get_active_session()
    logger.info("Snowflakeセッションを正常に取得しました")
except Exception as e:
    error_msg = f"Snowflakeセッションの取得に失敗しました: {e}"
    logger.error(error_msg)
    st.error(error_msg)
    st.info("このアプリは 'Streamlit in Snowflake' 環境での実行を想定しています。")
    st.stop()

# --- 用語定義 ---
JOIN_TYPES = {
    'INNER JOIN': '内部結合',
    'LEFT JOIN': '左外部結合'
}
AGG_FUNCS = {
    'none': '(なし)',
    'COUNT': '件数',
    'SUM': '合計',
    'AVG': '平均',
    'MIN': '最小',
    'MAX': '最大',
    'COUNT(DISTINCT)': '件数（重複なし）'
}
OPERATORS = {
    '=': '等しい',
    '!=': '等しくない',
    '>': 'より大きい',
    '>=': '以上',
    '<': 'より小さい',
    '<=': '以下',
    'LIKE_PARTIAL': '部分一致',
    'LIKE_FORWARD': '前方一致',
    'LIKE_BACKWARD': '後方一致',
    'IN': '複数選択',
    'IS NULL': '空の値',
    'IS NOT NULL': '空でない値'
}

# --- キャッシュ関数 ---
@st.cache_data(show_spinner=False)
def get_table_definition(db: Optional[str], schema: Optional[str], table: Optional[str]) -> pd.DataFrame:
    """指定されたテーブルの定義（カラム名とデータ型）をDataFrameで取得してキャッシュする"""
    if not all([db, schema, table]):
        return pd.DataFrame()
    
    # セキュリティチェック
    db = sanitize_identifier(db)
    schema = sanitize_identifier(schema)
    table = sanitize_identifier(table)
    
    if not all([db, schema, table]):
        logger.warning("無効な識別子が指定されました")
        return pd.DataFrame()
    
    try:
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM "{db}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION;
        """
        result = session.sql(query).to_pandas()
        logger.info(f"テーブル定義を正常に取得しました: {db}.{schema}.{table}")
        return result
    except Exception as e:
        handle_database_error("テーブル定義の取得", e)
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def get_qualified_table_columns(db: Optional[str], schema: Optional[str], table: Optional[str]) -> List[str]:
    """テーブル名で修飾されたカラムリストを取得してキャッシュする"""
    if not all([db, schema, table]):
        return []
    
    # セキュリティチェック
    db = sanitize_identifier(db)
    schema = sanitize_identifier(schema)
    table = sanitize_identifier(table)
    
    if not all([db, schema, table]):
        logger.warning("無効な識別子が指定されました")
        return []
    
    try:
        query = f'DESC TABLE "{db}"."{schema}"."{table}"'
        cols_df = session.sql(query).collect()
        result = [f'"{table}"."{c["name"]}"' for c in cols_df]
        logger.info(f"カラム情報を正常に取得しました: {db}.{schema}.{table} ({len(result)} columns)")
        return result
    except Exception as e:
        handle_database_error("カラム情報の取得", e)
        return []

# --- コールバック関数 ---
def add_item(session_state_key: str, parent_id: Optional[str] = None) -> None:
    """セッションステートにアイテムを追加する"""
    item_id = str(uuid.uuid4())
    new_item: Dict[str, Any] = {'id': item_id}

    if session_state_key == 'select_items':
        new_item['column'] = None
        new_item['agg_func'] = 'none'
        st.session_state.select_items.append(new_item)
    elif session_state_key == 'where_conditions':
        new_item['logical'] = 'AND'
        new_item['column'] = None
        new_item['operator'] = '='
        new_item['value'] = ''
        st.session_state.where_conditions.append(new_item)
    elif session_state_key == 'joins':
        new_item['type'] = 'INNER JOIN'
        new_item['right_table'] = None
        new_item['on_conditions'] = []
        st.session_state.joins.append(new_item)
    elif session_state_key == 'on_conditions':
        for join in st.session_state.joins:
            if join['id'] == parent_id:
                join['on_conditions'].append({
                    'id': item_id, 
                    'left_col': None, 
                    'right_col': None
                })
                break

def remove_item(session_state_key: str, item_id: str, parent_id: Optional[str] = None) -> None:
    """セッションステートからアイテムを削除する"""
    if parent_id:
        for join in st.session_state.joins:
            if join['id'] == parent_id:
                join['on_conditions'] = [
                    c for c in join['on_conditions'] 
                    if c.get('id') != item_id
                ]
                break
    else:
        st.session_state[session_state_key] = [
            item for item in st.session_state[session_state_key] 
            if item.get('id') != item_id
        ]

# --- UI描画関数 ---
def render_condition_builder(builder_title, session_state_key, all_columns):
    st.write(f"**{builder_title}**")
    
    op_display_list = list(OPERATORS.values())
    op_internal_list = list(OPERATORS.keys())
    
    for i, condition in enumerate(st.session_state[session_state_key]):
        cols = st.columns([1.5, 3, 2, 3, 1])
        if i > 0:
            condition['logical'] = cols[0].radio("", ("AND", "OR"), key=f"logic_{condition['id']}", horizontal=True, label_visibility="collapsed", help="前の条件とこの条件を「かつ(AND)」で繋ぐか、「または(OR)」で繋ぐか選択します。")
        else:
            cols[0].empty()
        condition['column'] = cols[1].selectbox("カラム", all_columns, key=f"col_{condition['id']}",
            index=all_columns.index(condition['column']) if condition.get('column') in all_columns else None,
            placeholder="条件を設定する列...", label_visibility="collapsed")
        
        current_op_index = op_internal_list.index(condition['operator']) if condition.get('operator') in op_internal_list else 0
        
        selected_op_display = cols[2].selectbox("演算子", op_display_list,
            key=f"op_disp_{condition['id']}", index=current_op_index, label_visibility="collapsed")
        condition['operator'] = op_internal_list[op_display_list.index(selected_op_display)]

        if condition['operator'] not in ['IS NULL', 'IS NOT NULL']:
            condition['value'] = cols[3].text_input("値", key=f"val_{condition['id']}", value=condition.get('value', ''), label_visibility="collapsed", help="比較したい値を入力します。複数選択の場合はカンマ区切りで指定します (例: A,B,C)")
        else:
            cols[3].empty()
        cols[4].button("🗑️", key=f"del_{condition['id']}", on_click=remove_item, args=(session_state_key, condition['id']), help="この条件を削除")
    st.button(f"➕ 条件を追加", key=f"add_{session_state_key}", on_click=add_item, args=(session_state_key,))

# --- 1. オブジェクトの選択 ---
with st.expander("STEP 1: データベース/スキーマ/テーブルを選択", expanded=True):
    try:
        db_df = session.sql("SHOW DATABASES").collect()
        db_names = [row['name'] for row in db_df]
    except Exception as e:
        st.error(f"データベースの取得に失敗しました: {e}"); db_names = []
    selected_db = st.selectbox('データベースを選択してください:', db_names, index=None, placeholder="データベースを選択...")

    selected_schema = None
    table_names = []
    if selected_db:
        try:
            schema_df = session.sql(f'SHOW SCHEMAS IN DATABASE "{selected_db}"').collect()
            schema_names = [row['name'] for row in schema_df if row['name'] not in ('INFORMATION_SCHEMA', 'PUBLIC')]
            selected_schema = st.selectbox('スキーマを選択してください:', schema_names, index=None, placeholder="スキーマを選択...")
            if selected_schema:
                 table_df = session.sql(f'SHOW TABLES IN SCHEMA "{selected_db}"."{selected_schema}"').collect()
                 table_names = [row['name'] for row in table_df]
        except Exception as e:
            st.warning(f"スキーマの取得中にエラーが発生しました: {e}")

    st.write("**主テーブル**", help="分析の起点となる、最も主要なテーブルを選択してください。")
    selected_table = st.selectbox('クエリの起点となるテーブルを選択してください:', table_names, index=None, placeholder="テーブルを選択...", label_visibility="collapsed")

    if selected_table:
        st.markdown("---")
        st.write("**テーブル結合 (JOIN)**", help="主テーブルに別のテーブルを繋げて、複数のテーブルをまたいだ分析を行いたい場合に追加します。")
        
        for i, join in enumerate(st.session_state.joins):
            st.markdown(f"**JOIN {i+1}**")
            cols = st.columns([2,3,1])
            
            tables_in_joins_so_far = [selected_table] + [j['right_table'] for j in st.session_state.joins if j['right_table']]
            available_tables_for_join = [t for t in table_names if t not in tables_in_joins_so_far or t == join.get('right_table')]
            
            join_display_list = list(JOIN_TYPES.values())
            join_internal_list = list(JOIN_TYPES.keys())
            current_join_index = join_internal_list.index(join['type']) if join.get('type') in join_internal_list else 0

            selected_join_display = cols[0].selectbox("結合タイプ", join_display_list, key=f"join_type_disp_{join['id']}", index=current_join_index, label_visibility="collapsed")
            join['type'] = join_internal_list[join_display_list.index(selected_join_display)]
            
            join['right_table'] = cols[1].selectbox("結合するテーブル", available_tables_for_join, key=f"join_table_{join['id']}", label_visibility="collapsed", placeholder="結合テーブルを選択...")
            cols[2].button("🗑️ この結合を削除", key=f"remove_join_{join['id']}", on_click=remove_item, args=('joins', join['id']))

            if join['right_table']:
                 st.write("ON句 (結合条件)")
                 st.caption("どの列を基準にテーブル同士を繋ぐかを指定します。通常は両テーブルに共通するIDやコードの列を選択します。")

                 tables_before_this_join = [selected_table]
                 for j_index in range(i):
                     prev_join = st.session_state.joins[j_index]
                     if prev_join.get('right_table'):
                         tables_before_this_join.append(prev_join['right_table'])
                
                 left_cols_options = []
                 for tbl in tables_before_this_join:
                     left_cols_options.extend(get_qualified_table_columns(selected_db, selected_schema, tbl))
                 
                 right_cols_options = get_qualified_table_columns(selected_db, selected_schema, join['right_table'])

                 for on_cond in join['on_conditions']:
                     on_cols = st.columns([4,1,4,1])
                     on_cond['left_col'] = on_cols[0].selectbox("左のテーブルのカラム", left_cols_options, key=f"on_left_{on_cond['id']}", label_visibility="collapsed")
                     on_cols[1].markdown("<p style='text-align: center; margin-top: 10px;'>=</p>", unsafe_allow_html=True)
                     on_cond['right_col'] = on_cols[2].selectbox("右のテーブルのカラム", right_cols_options, key=f"on_right_{on_cond['id']}", label_visibility="collapsed")
                     on_cols[3].button("🗑️", key=f"remove_on_{on_cond['id']}", on_click=remove_item, args=('on_conditions', on_cond['id'], join['id']))
                 st.button("➕ ON条件を追加", key=f"add_on_{join['id']}", on_click=add_item, args=('on_conditions', join['id']))
            st.markdown("---")

        if len(st.session_state.joins) < 2:
            st.button("➕ テーブルを結合", on_click=add_item, args=('joins',))

# --- 2. SQLの項目と条件の指定 ---
qualified_columns = []
is_aggregation_used = False
if selected_table:
    all_tables_in_query = [selected_table]
    if 'joins' in st.session_state:
        all_tables_in_query.extend([j['right_table'] for j in st.session_state.joins if j.get('right_table')])
    
    for tbl in list(set(all_tables_in_query)):
        qualified_columns.extend(get_qualified_table_columns(selected_db, selected_schema, tbl))

    with st.expander("STEP 2: SQLの項目と条件を指定", expanded=True):
        # テーブル定義の表示
        st.write("**テーブル定義**")
        valid_tables = [tbl for tbl in all_tables_in_query if tbl]
        if valid_tables:
            tab_list = st.tabs([f"📜 {tbl}" for tbl in valid_tables])
            for i, table_name in enumerate(valid_tables):
                with tab_list[i]:
                    definition_df = get_table_definition(selected_db, selected_schema, table_name)
                    if not definition_df.empty:
                        st.dataframe(definition_df, use_container_width=True)
                    else:
                        st.warning(f"{table_name} の定義を取得できませんでした。")
        else:
            st.info("STEP1でテーブルを選択すると、ここに定義が表示されます。")
        
        st.markdown("---")

        if not qualified_columns:
            st.warning("クエリ対象のカラムがありません。STEP1でテーブルを選択・設定してください。")
        else:
            st.write("**1. 表示・集計する項目を選択**")
            st.caption("集計方法で`(なし)`を選択した項目は、データをまとめる（GROUP BY）キーとして使用されます。")
            
            agg_display_list = list(AGG_FUNCS.values())
            agg_internal_list = list(AGG_FUNCS.keys())

            for item in st.session_state.select_items:
                cols = st.columns([4, 3, 1])
                item['column'] = cols[0].selectbox("カラム", qualified_columns, key=f"scol_{item['id']}",
                    index=qualified_columns.index(item['column']) if item.get('column') in qualified_columns else None,
                    label_visibility="collapsed", placeholder="表示したい列...")
                
                current_agg_index = agg_internal_list.index(item['agg_func']) if item.get('agg_func') in agg_internal_list else 0
                
                selected_agg_display = cols[1].selectbox("集計方法", agg_display_list,
                    key=f"agg_disp_{item['id']}", index=current_agg_index, label_visibility="collapsed")
                item['agg_func'] = agg_internal_list[agg_display_list.index(selected_agg_display)]

                cols[2].button("🗑️", key=f"sdel_{item['id']}", on_click=remove_item, args=('select_items', item['id']), help="この項目を削除")
            st.button("➕ 項目を追加", on_click=add_item, args=('select_items',))
            is_aggregation_used = any(item.get('agg_func') != 'none' for item in st.session_state.select_items)

            st.markdown("---")
            render_condition_builder("2. WHERE句の条件を指定 (集計前の絞り込み)", 'where_conditions', qualified_columns)

# --- 3. SQLの生成と実行 ---
def build_condition_clause(session_state_key: str, all_columns: List[str]) -> str:
    """条件句を構築する"""
    parts = []
    for i, cond in enumerate(st.session_state[session_state_key]):
        cond_str = ""
        if (cond.get('column') in all_columns and 
            (cond.get('operator') not in ['IS NULL', 'IS NOT NULL'] or cond.get('value') is not None)):
            
            op, val, col_name = cond['operator'], cond['value'], cond['column']
            
            # セキュリティチェック
            if not validate_sql_value(str(val)):
                logger.warning(f"危険な値が検出されました: {val}")
                continue
                
            if op in ['IS NULL', 'IS NOT NULL']:
                cond_str = f"{col_name} {op}"
            elif op == 'IN':
                items = [f"'{item.strip()}'" for item in str(val).split(',') if item.strip()]
                if items: 
                    cond_str = f"{col_name} IN ({', '.join(items)})"
            elif 'LIKE' in op:
                like_val = str(val).replace("'", "''")
                if op == 'LIKE_PARTIAL':
                    cond_str = f"{col_name} LIKE '%{like_val}%'"
                elif op == 'LIKE_FORWARD':
                    cond_str = f"{col_name} LIKE '{like_val}%'"
                elif op == 'LIKE_BACKWARD':
                    cond_str = f"{col_name} LIKE '%{like_val}'"
            else:
                # 数値かどうかをチェック
                if str(val).replace('.', '').replace('-', '').isdigit():
                    formatted_val = val
                else:
                    # シングルクォートをエスケープ
                    escaped_val = str(val).replace("'", "''")
                    formatted_val = f"'{escaped_val}'"
                cond_str = f"{col_name} {op} {formatted_val}"
                
        if cond_str:
            parts.append(f"{cond['logical']} {cond_str}" if i > 0 else cond_str)
    return "\n  ".join(parts) if parts else ""

def generate_sql_query(selected_db: str, selected_schema: str, selected_table: str, 
                      qualified_columns: List[str], is_aggregation_used: bool) -> str:
    """SQLクエリを生成する"""
    try:
        # SELECT句の構築
        select_parts = []
        groupby_parts = []
        for item in st.session_state.select_items:
            if item.get('column') in qualified_columns:
                col_name = item["column"]
                agg_func = item.get('agg_func', 'none')
                if agg_func == 'none':
                    select_parts.append(col_name)
                    if is_aggregation_used:
                        groupby_parts.append(col_name)
                elif agg_func == 'COUNT(DISTINCT)':
                    select_parts.append(f"COUNT(DISTINCT {col_name})")
                else:
                    select_parts.append(f"{agg_func}({col_name})")
        
        select_clause = ",\n    ".join(select_parts) if select_parts else "*"
        
        # FROM句の構築
        full_from_clause = f'FROM "{selected_db}"."{selected_schema}"."{selected_table}"'
        for join in st.session_state.joins:
            if join.get('right_table') and join.get('on_conditions'):
                on_clause_parts = []
                join_type_str = join["type"]
                for on_cond in join['on_conditions']:
                    if on_cond.get('left_col') and on_cond.get('right_col'):
                        on_clause_parts.append(f"{on_cond['left_col']} = {on_cond['right_col']}")
                if on_clause_parts:
                    full_from_clause += f'\n  {join_type_str} "{selected_db}"."{selected_schema}"."{join["right_table"]}"\n    ON {" AND ".join(on_clause_parts)}'
        
        # SQLクエリの組み立て
        generated_sql = f"SELECT\n    {select_clause}\n{full_from_clause}"

        # WHERE句の追加
        where_clause = build_condition_clause('where_conditions', qualified_columns)
        if where_clause: 
            generated_sql += f"\nWHERE\n  {where_clause}"
        
        # GROUP BY句の追加
        if is_aggregation_used and groupby_parts:
            generated_sql += f"\nGROUP BY\n    {', '.join(groupby_parts)}"
        
        generated_sql += ";"
        
        logger.info("SQLクエリを正常に生成しました")
        return generated_sql
        
    except Exception as e:
        error_msg = f"SQLクエリの生成中にエラーが発生しました: {e}"
        logger.error(error_msg)
        st.error(error_msg)
        return ""

def execute_sample_query(sql_query: str) -> Optional[pd.DataFrame]:
    """サンプルクエリを実行する"""
    if not sql_query:
        return None
        
    try:
        execution_sql = sql_query.rstrip(';') + " LIMIT 10;"
        result_df = session.sql(execution_sql).to_pandas()
        logger.info("サンプルクエリを正常に実行しました")
        return result_df
    except Exception as e:
        handle_database_error("サンプルクエリの実行", e)
        return None

if selected_table:
    with st.expander("STEP 3: SQLを確認して実行", expanded=True):
        # SQLクエリの生成
        if selected_schema:  # selected_schemaがNoneでないことを確認
            generated_sql = generate_sql_query(
                selected_db, selected_schema, selected_table, 
                qualified_columns, is_aggregation_used
            )
            
            if generated_sql:
                st.write("#### 生成されたSQL")
                st.code(generated_sql, language='sql')
                
                if st.button('🚀 サンプルクエリを実行 (10行)', type="primary"):
                    st.write("#### 実行結果 (最初の10行)")
                    with st.spinner('クエリを実行中です...'):
                        result_df = execute_sample_query(generated_sql)
                        if result_df is not None:
                            st.dataframe(result_df, use_container_width=True)
                            st.success(f"最大10行のサンプルデータを取得しました。")
            else:
                st.error("SQLクエリの生成に失敗しました。設定を確認してください。")
        else:
            st.error("スキーマが選択されていません。STEP1でスキーマを選択してください。")
