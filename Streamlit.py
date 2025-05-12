import streamlit as st
import snowflake.connector
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import math

# === Set Streamlit layout ===
st.set_page_config(layout="wide")

# === Custom Styling ===
st.markdown("""
<style>
.stButton>button {
    background-color: #6A4E93;
    color: white;
    border-radius: 5px;
    padding: 8px 16px;
    font-size: 16px;
}
.stButton>button:hover {
    background-color: #5A3E7E;
}
.tab-title {
    font-size: 24px;
    color: #6A4E93;
    font-weight: 600;
    margin-bottom: 0;
}
.ag-theme-balham {
    overflow-x: auto;
}
</style>
""", unsafe_allow_html=True)

# === Secure DB Connection using secrets.toml ===
sf = st.secrets["snowflake"]

def get_connection():
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"]
    )

# === Load Data ===
def get_data(table):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df

# === Update Changed Cells ===
def update_changed_cells(original_df, edited_df, table_name, unique_col):
    original_df = original_df.reset_index(drop=True)
    edited_df = edited_df.reset_index(drop=True)
    updated_keys = set()
    changes = []

    for i in range(len(original_df)):
        original_row = original_df.loc[i]
        edited_row = edited_df.loc[i]
        row_changes = {}
        for col in original_df.columns:
            if pd.isna(original_row[col]) and pd.isna(edited_row[col]):
                continue
            if original_row[col] != edited_row[col]:
                row_changes[col] = edited_row[col]
        if row_changes:
            updated_keys.add(original_row[unique_col])
            for col, new_val in row_changes.items():
                changes.append({
                    "key": original_row[unique_col],
                    "column": col,
                    "new_value": new_val
                })

    if changes:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            for change in changes:
                query = f"""
                    UPDATE {table_name}
                    SET {change['column']} = %s
                    WHERE {unique_col} = %s
                """
                cursor.execute(query, (change["new_value"], change["key"]))
            conn.commit()
            st.success(f"✅ {len(updated_keys)} row(s) updated.")
        except Exception as e:
            st.error(f"❌ Error: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        st.info("No changes to save.")

# === Add Column ===
def add_column_to_snowflake(table_name, column_name):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} STRING")
        conn.commit()
        st.success(f"✅ Column '{column_name}' added.")
    except Exception as e:
        st.error(f"❌ Failed to add column: {e}")
    finally:
        cursor.close()
        conn.close()

# === Tabs ===
tab1 = st.tabs(["📊 Structure"])[0]

# === Structure Tab ===
with tab1:
    st.markdown('<div class="tab-title">Structure Table</div>', unsafe_allow_html=True)

    if "show_add_column_modal" not in st.session_state:
        st.session_state["show_add_column_modal"] = False

    col1, col2 = st.columns([0.95, 0.05])
    with col2:
        if st.button("\u22EE"):
            st.session_state["show_add_column_modal"] = not st.session_state["show_add_column_modal"]

    if st.session_state["show_add_column_modal"]:
        with st.form("add_column_form", clear_on_submit=True):
            st.subheader("➕ Add New Column")
            new_column = st.text_input("Enter new column name")
            submitted = st.form_submit_button("Add Column")
            if submitted:
                if new_column:
                    add_column_to_snowflake("SampleTest", new_column)
                    st.session_state["show_add_column_modal"] = False
                    st.rerun()
                else:
                    st.warning("Please enter a valid column name.")

    df1 = get_data("SampleTest")
    search = st.text_input("🔍 Enter filter keyword")

    if search:
        df1_filtered = df1[df1.apply(lambda row: row.astype(str).str.contains(search, case=False).any(), axis=1)]
    else:
        df1_filtered = df1

    # === Pagination Setup ===
    rows_per_page = 3
    total_rows = len(df1_filtered)
    total_pages = math.ceil(total_rows / rows_per_page)

    if "page_structure" not in st.session_state:
        st.session_state.page_structure = 0

    start_idx = st.session_state.page_structure * rows_per_page
    end_idx = start_idx + rows_per_page
    df1_page = df1_filtered.iloc[start_idx:end_idx]

    gb = GridOptionsBuilder.from_dataframe(df1_page)
    gb.configure_default_column(
        editable=True,
        sortable=True,
        filter=True,
        resizable=True,
        wrapHeaderText=True,
        autoHeaderHeight=True
    )
    gb.configure_grid_options(domLayout='normal', headerHeight=60)
    grid_options = gb.build()

    grid_response = AgGrid(
        df1_page,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        fit_columns_on_grid_load=False,
        height=len(df1_page) * 35 + 60,
        allow_unsafe_jscode=True,
        theme="balham"
    )

    edited_df1 = grid_response["data"]

    if st.button("💾 Save Changes to Snowflake (Structure)"):
        update_changed_cells(df1_filtered, edited_df1, "SampleTest", "CODE")

    csv = edited_df1.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", data=csv, file_name="structure_data.csv", mime="text/csv")

    # === Pagination Buttons at Bottom ===
    col_prev, col_page, col_next = st.columns([1, 2, 1])
    with col_prev:
        if st.button("⬅️ Previous", key="prev_structure") and st.session_state.page_structure > 0:
            st.session_state.page_structure -= 1
            st.rerun()
    with col_page:
        st.markdown(f"**Page {st.session_state.page_structure + 1} of {total_pages}**", unsafe_allow_html=True)
    with col_next:
        if st.button("➡️ Next", key="next_structure") and st.session_state.page_structure < total_pages - 1:
            st.session_state.page_structure += 1
            st.rerun()
