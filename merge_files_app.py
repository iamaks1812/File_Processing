import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
from collections import Counter

st.set_page_config(page_title="Advanced Data Extractor", layout="wide")
st.title("📁 Merge Files + Transform + Extract")

# ✅ Session setup (persists unless Start Over)
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = "uploader_1"
if "lookup_done" not in st.session_state:
    st.session_state.lookup_done = False

# 🔁 Start Over resets all
if st.button("🔁 Start Over"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.uploader_key = f"uploader_{pd.Timestamp.now().timestamp()}"
    st.rerun()

# File uploader
uploaded_files = st.file_uploader(
    "Upload multiple CSV/Excel files (Max 1 GB each)",
    type=["csv", "xlsx"],
    accept_multiple_files=True,
    key=st.session_state.uploader_key
)

# Helper functions
def deduplicate_columns(cols):
    seen = Counter()
    new_cols = []
    for col in cols:
        if seen[col]:
            new_cols.append(f"{col}_{seen[col]}")
        else:
            new_cols.append(col)
        seen[col] += 1
    return new_cols

def fix_dataframe_arrow(df):
    for col in df.columns:
        try:
            df.loc[:, col] = df[col].astype(str)
        except Exception:
            pass
    return df

def generate_download(df, filename, format):
    if not filename.lower().endswith(f".{format}"):
        filename += f".{format}"
    if format == "xlsx":
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Data")
        return buffer.getvalue(), filename
    else:
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        return buffer.getvalue().encode(), filename

merged_df = pd.DataFrame()

# 🧩 Merge logic
if uploaded_files:
    for idx, file in enumerate(uploaded_files):
        if file.size > 1073741824:
            st.error(f"❌ File '{file.name}' exceeds 1 GB limit.")
            continue

        try:
            df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        except Exception as e:
            st.error(f"Error reading {file.name}: {e}")
            continue

        df.columns = ["" if "Unnamed" in str(col) else str(col).strip() for col in df.columns]
        if idx > 0:
            df = df.iloc[1:].reset_index(drop=True)

        if not merged_df.empty and not df.columns.equals(merged_df.columns):
            st.warning(f"⚠️ File '{file.name}' has different columns. Skipped.")
            continue

        merged_df = pd.concat([merged_df, df], ignore_index=True)

    if not merged_df.empty:
        st.success("✅ Files merged successfully!")
        st.dataframe(fix_dataframe_arrow(merged_df.head()))

        # 📄 Custom merged file naming
        merged_filename = st.text_input("📄 Filename for merged file:", "merged_file")
        merged_format = st.selectbox("📦 Format for merged file:", ["xlsx", "csv"], key="merged_format")
        merged_bytes, merged_download_name = generate_download(merged_df, merged_filename, merged_format)
        st.download_button("⬇️ Download Merged File", merged_bytes, merged_download_name)

        # 🔄 Mapping section
        st.subheader("🔄 Multi-Column Lookup Mapping")
        mapping_file = st.file_uploader("Upload mapping file", type=["csv", "xlsx"], key="map_file")

        if mapping_file:
            try:
                map_df = pd.read_csv(mapping_file) if mapping_file.name.endswith(".csv") else pd.read_excel(mapping_file)
                map_df.columns = deduplicate_columns(map_df.columns)
                st.write("📎 Mapping Columns:", list(map_df.columns))

                key_col_main = st.selectbox("Key column in merged file", options=merged_df.columns)
                key_col_map = st.selectbox("Key column in mapping file", options=map_df.columns)
                value_cols_to_map = st.multiselect(
                    "Select one or more columns to bring in (in order)",
                    options=[col for col in map_df.columns if col != key_col_map]
                )
                method = st.radio("Lookup Method:", ["merge", "map"])

                if st.button("🔁 Perform Lookup"):
                    merged_df[key_col_main] = merged_df[key_col_main].astype(str).str.strip().str.upper()
                    map_df[key_col_map] = map_df[key_col_map].astype(str).str.strip().str.upper()

                    mapped_columns = []

                    for value_col_map in value_cols_to_map:
                        new_col_name = f"{value_col_map}_mapped"
                        try:
                            if method == "merge":
                                temp_df = map_df[[key_col_map, value_col_map]].rename(columns={value_col_map: new_col_name})
                                merged_df = merged_df.merge(temp_df, left_on=key_col_main, right_on=key_col_map, how="left")
                                merged_df[new_col_name] = merged_df.get(new_col_name, "Not Matched").fillna("Not Matched")
                            else:
                                lookup_dict = dict(zip(map_df[key_col_map], map_df[value_col_map]))
                                merged_df[new_col_name] = merged_df[key_col_main].map(lookup_dict).fillna("Not Matched")

                            mapped_columns.append(new_col_name)
                        except Exception as e:
                            st.error(f"Mapping failed for column '{value_col_map}': {e}")

                    for col in reversed(mapped_columns):
                        col_data = merged_df.pop(col)
                        merged_df.insert(0, col, col_data)

                    st.session_state.lookup_done = True
                    st.success("✅ Lookup completed.")
                    st.dataframe(fix_dataframe_arrow(merged_df[mapped_columns + [key_col_main]].head()))

            except Exception as e:
                st.error(f"Error reading mapping file: {e}")

# ✅ Show this only when lookup was performed
if st.session_state.lookup_done:
    lookup_filename = st.text_input("📄 Filename for lookup file:", "after_lookup")
    lookup_format = st.selectbox("📦 Format for lookup file:", ["xlsx", "csv"], key="lookup_format")
    lookup_bytes, lookup_download_name = generate_download(merged_df, lookup_filename, lookup_format)
    st.download_button("⬇️ Download File After Lookup", lookup_bytes, lookup_download_name)
