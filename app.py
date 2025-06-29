# 必要なライブラリをインポート
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid

# --- Streamlitアプリの基本設定 ---
st.set_page_config(layout="wide")
st.title('初心者向けSQLジェネレーター')


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
except Exception as e:
    st.error(f"Snowflakeセッションの取得に失敗しました: {e}")
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
def get_table_definition(db, schema, table):
    """指定されたテーブルの定義（カラム名とデータ型）をDataFrameで取得してキャッシュする"""
    if not all([db, schema, table]):
        return pd.DataFrame()
    try:
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM "{db}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION;
        """
        return session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def get_qualified_table_columns(db, schema, table):
    """テーブル名で修飾されたカラムリストを取得してキャッシュする"""
    if not all([db, schema, table]):
        return []
    try:
        query = f'DESC TABLE "{db}"."{schema}"."{table}"'
        cols_df = session.sql(query).collect()
        return [f'"{table}"."{c["name"]}"' for c in cols_df]
    except Exception:
        return []

# --- コールバック関数 ---
def add_item(session_state_key, parent_id=None):
    item_id = str(uuid.uuid4())
    new_item = {'id': item_id}

    if session_state_key == 'select_items':
        new_item.update({'column': None, 'agg_func': 'none'})
        st.session_state.select_items.append(new_item)
    elif session_state_key == 'where_conditions':
        new_item.update({'logical': 'AND', 'column': None, 'operator': '=', 'value': ''})
        st.session_state.where_conditions.append(new_item)
    elif session_state_key == 'joins':
        new_item.update({'type': 'INNER JOIN', 'right_table': None, 'on_conditions': []})
        st.session_state.joins.append(new_item)
    elif session_state_key == 'on_conditions':
        for join in st.session_state.joins:
            if join['id'] == parent_id:
                join['on_conditions'].append({'id': item_id, 'left_col': None, 'right_col': None})
                break

def remove_item(session_state_key, item_id, parent_id=None):
    if parent_id:
        for join in st.session_state.joins:
            if join['id'] == parent_id:
                join['on_conditions'] = [c for c in join['on_conditions'] if c.get('id') != item_id]
                break
    else:
        st.session_state[session_state_key] = [
            item for item in st.session_state[session_state_key] if item.get('id') != item_id
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
def build_condition_clause(session_state_key, all_columns):
    parts = []
    for i, cond in enumerate(st.session_state[session_state_key]):
        cond_str = ""
        if cond.get('column') in all_columns and (cond.get('operator') not in ['IS NULL', 'IS NOT NULL'] or cond.get('value') is not None):
            op, val, col_name = cond['operator'], cond['value'], cond['column']
            if op in ['IS NULL', 'IS NOT NULL']:
                cond_str = f"{col_name} {op}"
            elif op == 'IN':
                items = [f"'{item.strip()}'" for item in str(val).split(',') if item.strip()]
                if items: cond_str = f"{col_name} IN ({', '.join(items)})"
            elif 'LIKE' in op:
                like_val = str(val).replace("'", "''")
                if op == 'LIKE_PARTIAL':
                    cond_str = f"{col_name} LIKE '%{like_val}%'"
                elif op == 'LIKE_FORWARD':
                    cond_str = f"{col_name} LIKE '{like_val}%'"
                elif op == 'LIKE_BACKWARD':
                    cond_str = f"{col_name} LIKE '%{like_val}'"
            else:
                formatted_val = val if str(val).isnumeric() or (str(val).startswith("'") and str(val).endswith("'")) else f"'{val}'"
                cond_str = f"{col_name} {op} {formatted_val}"
        if cond_str:
            parts.append(f"{cond['logical']} {cond_str}" if i > 0 else cond_str)
    return "\n  ".join(parts) if parts else ""

if selected_table:
    with st.expander("STEP 3: SQLを確認して実行", expanded=True):
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
        
        generated_sql = f"SELECT\n    {select_clause}\n{full_from_clause}"

        where_clause = build_condition_clause('where_conditions', qualified_columns)
        if where_clause: generated_sql += f"\nWHERE\n  {where_clause}"
        
        if is_aggregation_used and groupby_parts:
            generated_sql += f"\nGROUP BY\n    {', '.join(groupby_parts)}"
        
        generated_sql += ";"

        st.write("#### 生成されたSQL"); st.code(generated_sql, language='sql')
        if st.button('🚀 サンプルクエリを実行 (10行)', type="primary"):
            st.write("#### 実行結果 (最初の10行)")
            with st.spinner('クエリを実行中です...'):
                try:
                    execution_sql = generated_sql.rstrip(';') + " LIMIT 10;"
                    result_df = session.sql(execution_sql).to_pandas()
                    st.dataframe(result_df, use_container_width=True)
                    st.success(f"最大10行のサンプルデータを取得しました。")
                except Exception as e:
                    st.error(f"クエリの実行中にエラーが発生しました:\n{e}")
