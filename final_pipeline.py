import sqlite3
import pandas as pd
import streamlit as st
import google.generativeai as genai
import os, re, json, math, time
import threading
import chromadb
from chromadb.utils import embedding_functions

# ══════════════════════════════════════════
# 1. CONFIGURATION
# ══════════════════════════════════════════
class Config:
    API_KEYS_RAW = os.getenv("GEMINI_API_KEYS", "")
    API_KEY = os.getenv("GEMINI_API_KEY", "")
    DB_PATH = 'frammer_analytics.db'
    FEW_SHOT_PATH = 'few_shot_examples.json'
    MAX_RETRIES = 2
    TOP_K = 3
    RAG_TOP_TABLES = 5
    DATA_DIR = r"C:\Users\nanoh\OneDrive\Desktop\GC DATA\synthetic_data_CTGANs\synthetic_data_CTGANs"
    JSON_CONFIG = {"response_mime_type": "application/json"}
    
    TABLE_DESCRIPTIONS = {
        "dim_channel_client":           "Channels and parent media companies. Clients and Brands.",
        "dim_date":                     "Calendar dimension. Month 1-12, Quarter 1-4. Time, dates, years, weeks.",
        "dim_input_type":               "Type of raw video. Values: interview, news bulletin, special reports, speech, debate, podcast.",
        "dim_input_video":              "Input video metadata: headline, source name, source URL.",
        "dim_language":                 "Language lookup. Values: en, hi, mr, mix, es, ar. Hindi, English.",
        "dim_output_type":              "Output clip format. Values: Full Package, Key moments, Chapters, Summary.",
        "dim_output_video":             "Output video headline only. Published_URL lives in fact_publish.",
        "dim_platform":                 "Platform lookup. YouTube, Facebook, Instagram, LinkedIn, Reels, Shorts, X, Threads.",
        "dim_user_team":                "Users and their teams. Uploaders, staff.",
        "fact_data_quality":            "Error log. failures, missing audio, corrupted files.",
        "fact_input_video_operations":  "PIPELINE STEP 1 — Every upload event. Uploads, raw files, initial ingestion. Durations are HH:MM:SS.",
        "fact_output_video_operations": "PIPELINE STEP 2 — Every processed clip. Edits, cut video, final creations. Durations are HH:MM:SS.",
        "fact_publish_information":     "PIPELINE STEP 3 — Every publish event. Social media posting, going live, billing. Durations are HH:MM:SS."
    }
    
    KPI_DEFINITIONS = {
        "Chapters": "Measures segmented content creation.",
        "Key Moments": "Tracks highlight clip generation.",
        "Summary": "Measures short-form summarized outputs.",
        "My Key Moments": "Tracks user-customized highlights.",
        "Translation Rate": "Shows percentage of content translated to other languages.",
        "Translation Uplift": "Measures value gained from translation.",
        "Videos Uploaded": "Shows total content entering the AI pipeline.",
        "Videos Processed": "Measures how much content AI successfully processes.",
        "Upload to Process Rate": "Indicates efficiency of the processing stage.",
        "Process to Publish Rate": "Shows how much processed content gets published.",
        "End-to-End Rate": "Measures overall pipeline effectiveness.",
        "Publish Rate": "Indicates final publishing success.",
        "Health Score": "Summarizes overall system performance.",
        "Recoverable Hours": "Shows lost processing time that could be recovered.",
        "Billable Upside": "Indicates potential revenue from unused output.",
        "Stalled Outputs": "Detects content stuck in the pipeline.",
        "Average Publish Latency": "Measures delay between processing and publishing.",
        "Clip Multiplication": "Shows how many clips AI generates per video.",
        "Duration Compression": "Measures reduction from long videos to short clips.",
        "Platforms Reached": "Indicates distribution across publishing platforms.",
        "Data Completeness": "Shows reliability and quality of dataset.",
        "Publishing Dropout": "Detects content lost before publishing.",
        "Content Reuse Ratio": "Measures how well content is repurposed.",
        "Full Repurposing Rate": "Shows inputs converted to all output types.",
        "Weekend Publish Rate": "Tracks publishing behavior on weekends.",
        "Upload Duration / Long Video Uploads": "Indicates demand for AI video processing.",
        "Processing Time": "Measures speed of AI clip processing.",
        "Active Clients": "Indicates number of clients using the platform.",
        "Active Channels": "Shows number of active publishing channels.",
        "Total Videos": "Shows total content volume in the system.",
        "Total Duration": "Measures total hours of content processed.",
        "Videos Published": "Indicates total successful outputs.",
        "Drop-off %": "Shows percentage of content lost in pipeline.",
        "Avg Upload to Process Time": "Measures delay from upload to AI processing.",
        "Avg Process to Publish Time": "Measures delay from processing to publishing.",
        "Full Package": "Tracks full-length content output volume."
    }

    FK_MAP = {
        "fact_input_video_operations": {
            "Input_Video_ID": ("dim_input_video",   "Input_Video_ID"),
            "Channel_ID":     ("dim_channel_client", "Channel_ID"),
            "User_ID":        ("dim_user_team",      "User_ID"),
            "Input_Type_ID":  ("dim_input_type",     "Input_Type_ID"),
            "Output_Type_ID": ("dim_output_type",    "Output_Type_ID"),
            "Language_ID":    ("dim_language",       "Language_ID"),
            "Upload_Date_ID": ("dim_date",           "Date_ID"),
        },
        "fact_output_video_operations": {
            "Output_Video_ID": ("dim_output_video",             "Output_Video_ID"),
            "Output_Type_ID":  ("dim_output_type",              "Output_Type_ID"),
            "Language_ID":     ("dim_language",                 "Language_ID"),
            "Created_Date_ID": ("dim_date",                     "Date_ID"),
            "Fact_Input_ID":   ("fact_input_video_operations",  "Fact_Input_ID"),
        },
        "fact_publish_information": {
            "Platform_ID":       ("dim_platform",                 "Platform_ID"),
            "Published_Date_ID": ("dim_date",                     "Date_ID"),
            "Fact_Output_ID":    ("fact_output_video_operations", "Fact_Output_ID"),
        },
        "fact_data_quality": {
            "Video_ID":    ("dim_input_video", "Input_Video_ID"),
            "Log_Date_ID": ("dim_date", "Date_ID"),
        },
    }

    PK_MAP = {
        "dim_channel_client": "Channel_ID", "dim_date": "Date_ID", "dim_input_type": "Input_Type_ID",
        "dim_input_video": "Input_Video_ID", "dim_language": "Language_ID", "dim_output_type": "Output_Type_ID",
        "dim_output_video": "Output_Video_ID", "dim_platform": "Platform_ID", "dim_user_team": "User_ID",
        "fact_input_video_operations": "Fact_Input_ID", "fact_output_video_operations": "Fact_Output_ID",
        "fact_publish_information": "Fact_Publish_ID", "fact_data_quality": "DQ_Log_ID"
    }

# ══════════════════════════════════════════
# 2. DATABASE MANAGER
# ══════════════════════════════════════════
_API_KEY_ROTATION_LOCK = threading.Lock()
_API_KEY_ROTATION_INDEX = 0


def _parse_api_keys() -> list[str]:
    values: list[str] = []
    for raw_value in (Config.API_KEYS_RAW, Config.API_KEY):
        if not raw_value:
            continue
        values.extend(part.strip() for part in re.split(r"[\r\n,]+", raw_value) if part.strip())
    return list(dict.fromkeys(values))


def _next_key_order(api_keys: list[str]) -> list[str]:
    if len(api_keys) <= 1:
        return api_keys
    global _API_KEY_ROTATION_INDEX
    with _API_KEY_ROTATION_LOCK:
        start_index = _API_KEY_ROTATION_INDEX % len(api_keys)
        _API_KEY_ROTATION_INDEX += 1
    return api_keys[start_index:] + api_keys[:start_index]


def _is_rate_limit_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    markers = ("429", "rate limit", "too many requests", "resource_exhausted", "quota", "retry_delay")
    return any(marker in lowered for marker in markers)


def _generate_content(prompt: str, *, json_mode: bool = False):
    api_keys = _parse_api_keys()
    if not api_keys:
        raise RuntimeError("Gemini API keys are not configured. Set GEMINI_API_KEYS or GEMINI_API_KEY.")

    last_error: Exception | None = None
    generation_config = Config.JSON_CONFIG if json_mode else None
    for api_key in _next_key_order(api_keys):
        try:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.5-flash', generation_config=generation_config)
            return model.generate_content(prompt)
        except Exception as exc:
            last_error = exc

    if last_error and _is_rate_limit_error(last_error):
        raise RuntimeError("The AI service is temporarily busy. Please try again in a moment.")
    raise RuntimeError("The AI service is temporarily unavailable. Please try again shortly.")


class DatabaseManager:
    @staticmethod
    def initialize_database():
        if os.path.exists(Config.DB_PATH):
            try:
                check_conn = sqlite3.connect(Config.DB_PATH)
                table_count = check_conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
                check_conn.close()
                if table_count > 0: return
            except Exception:
                pass  
        conn = sqlite3.connect(Config.DB_PATH)
        csv_files = [
            "dim_channel_client.csv", "dim_date.csv", "dim_input_type.csv", "dim_input_video.csv",
            "dim_language.csv", "dim_output_type.csv", "dim_output_video.csv", "dim_platform.csv",
            "dim_user_team.csv", "fact_input_video_operations.csv", "fact_output_video_operations.csv",
            "fact_publish_information.csv", "fact_data_quality.csv"
        ]
        for file in csv_files:
            path = os.path.join(Config.DATA_DIR, file)
            if os.path.exists(path):
                table_name = file.replace('.csv', '')
                df = pd.read_csv(path)
                if table_name == 'fact_input_video_operations':
                    df['User_ID']    = df['User_ID'].astype(str)
                    df['Channel_ID'] = df['Channel_ID'].astype(str)
                df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

    @staticmethod
    def build_schema_with_pragma(conn, selected_tables=None) -> str:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        all_tables = [row[0] for row in cursor.fetchall()]
        tables = [t for t in selected_tables if t in all_tables] if selected_tables else all_tables

        lines = []
        for table in tables:
            desc = Config.TABLE_DESCRIPTIONS.get(table, "Core Fact Table.")
            lines.append(f"-- {desc}")

            cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
            pk_col = Config.PK_MAP.get(table)
            table_fks = Config.FK_MAP.get(table, {})
            actual_col_names = {c[1] for c in cols} 

            col_defs = []
            for c in cols:
                col_name, col_type = c[1], (c[2] or "VARCHAR")
                suffix = " PRIMARY KEY" if col_name == pk_col else ""
                col_defs.append(f"    {col_name} {col_type}{suffix}")

            fk_defs = []
            for fk_col, (ref_table, ref_col) in table_fks.items():
                if fk_col in actual_col_names and ref_table in tables:
                    fk_defs.append(f"    FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col})")

            lines.append(f"CREATE TABLE {table} (")
            lines.append(",\n".join(col_defs + fk_defs))
            lines.append(");\n")

            try:
                samples = conn.execute(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 3").fetchall()
                if samples:
                    lines.append(f"-- Sample rows from {table}:")
                    for row in samples:
                        safe_row = [str(x)[:50] + ('...' if len(str(x)) > 50 else '') for x in row]
                        lines.append(f"-- {tuple(safe_row)}")
                    lines.append("")
            except Exception:
                pass

        return "\n".join(lines)

# ══════════════════════════════════════════
# 3. KNOWLEDGE BASE
# ══════════════════════════════════════════
class KnowledgeBase:
    @staticmethod
    @st.cache_resource
    def setup_chroma_db():
        client = chromadb.PersistentClient(path="chroma_db_cache")
        ef = embedding_functions.DefaultEmbeddingFunction()
        schema_collection = client.get_or_create_collection("schema_rag", embedding_function=ef)
        
        conn = sqlite3.connect(Config.DB_PATH)
        ids, docs = [], []
        for table, desc in Config.TABLE_DESCRIPTIONS.items():
            try:
                cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
                col_names = [c[1] for c in cols]
                rich_desc = f"{table}: {desc}\nColumns: {', '.join(col_names)}"
            except Exception:
                rich_desc = f"{table}: {desc}"
            ids.append(table)
            docs.append(rich_desc)
        conn.close()
        
        schema_collection.upsert(documents=docs, ids=ids)
        
        kpi_collection = client.get_or_create_collection("kpi_rag", embedding_function=ef)
        kpi_ids = list(Config.KPI_DEFINITIONS.keys())
        kpi_docs = [f"KPI Name: {k}\nDefinition: {v}" for k, v in Config.KPI_DEFINITIONS.items()]
        kpi_collection.upsert(documents=kpi_docs, ids=kpi_ids)
        
        return schema_collection, kpi_collection

    @staticmethod
    def retrieve_relevant_tables(question: str, collection, k=Config.RAG_TOP_TABLES) -> list:
        results = collection.query(query_texts=[question], n_results=k)
        return results['ids'][0] if results['ids'] else []

    @staticmethod
    def expand_schema_graph(seed_tables: list) -> list:
        expanded = set(seed_tables)
        added_new = True
        while added_new:
            added_new = False
            current_tables = list(expanded)
            for table in current_tables:
                for fact_table, references in Config.FK_MAP.items():
                    for fk_col, (ref_table, ref_col) in references.items():
                        if ref_table in expanded and fact_table not in expanded:
                            expanded.add(fact_table)
                            added_new = True
                        if fact_table in expanded and ref_table not in expanded:
                            expanded.add(ref_table)
                            added_new = True
        return list(expanded)

# ══════════════════════════════════════════
# 4. BM25 RETRIEVAL
# ══════════════════════════════════════════
class BM25Retriever:
    @staticmethod
    @st.cache_data
    def load_examples():
        if not os.path.exists(Config.FEW_SHOT_PATH): return []
        with open(Config.FEW_SHOT_PATH) as f: return json.load(f)

    @staticmethod
    def _tokenize(text: str) -> list: 
        return re.sub(r'[^a-z0-9 ]', '', text.lower()).split()

    @staticmethod
    def _build_index(corpus: list, k1: float = 1.5, b: float = 0.75) -> dict:
        tokenized = [BM25Retriever._tokenize(doc) for doc in corpus]
        N, avgdl = len(tokenized), sum(len(d) for d in tokenized) / max(len(tokenized), 1)
        df = {}
        for doc in tokenized:
            for word in set(doc): df[word] = df.get(word, 0) + 1
        idf = {word: math.log((N - freq + 0.5) / (freq + 0.5) + 1) for word, freq in df.items()}
        return {"tokenized": tokenized, "idf": idf, "avgdl": avgdl, "k1": k1, "b": b}

    @staticmethod
    def _score(query_tokens: list, index: dict) -> list:
        tokenized, idf, avgdl, k1, b = index["tokenized"], index["idf"], index["avgdl"], index["k1"], index["b"]
        scores = []
        for doc_tokens in tokenized:
            dl, freq = len(doc_tokens), {}
            for word in doc_tokens: freq[word] = freq.get(word, 0) + 1
            score = sum((idf[w] * freq.get(w, 0) * (k1 + 1)) / (freq.get(w, 0) + k1 * (1 - b + b * dl / avgdl)) for w in query_tokens if w in idf)
            scores.append(score)
        return scores

    @staticmethod
    def retrieve(query: str, examples: list, k: int = Config.TOP_K) -> list:
        if not examples: return []
        if len(examples) <= k: return examples
        index = BM25Retriever._build_index([ex["question"] for ex in examples])
        scores = BM25Retriever._score(BM25Retriever._tokenize(query), index)
        top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]
        return [examples[i] for i in top_indices]

# ══════════════════════════════════════════
# 5. SEMANTIC ENGINE
# ══════════════════════════════════════════
class PromptManager:
    ROUTER_PROMPT = """
You are an Intent Router. Analyze the user's question and route it to either:
1. "SQL" - The user is asking for data, numbers, counts, complex queries, or metrics from the database (e.g. "How many...", "Which channel...").
2. "KPI" - The user is asking for a definition, meaning, or explanation of a specific KPI term (e.g. "What is Full Package?", "Explain Clip Multiplication", "What does Translation Rate mean?").

Return strictly JSON:
{
  "route": "SQL" | "KPI",
  "reason": "..."
}
"""
    PLANNER_PROMPT = """
You are a query planning expert for Frammer AI. Analyse the question and produce an execution plan.

COMPLEXITY RULES:
  SIMPLE — touches 1 fact table + its dimensions.
  MULTI_STEP — crosses 2+ fact tables OR requires CTE.

Provide Output strictly matching this JSON schema:
{
  "complexity": "SIMPLE" | "MULTI_STEP",
  "plan": [{"step": 1, "description": "...", "tables": ["..."]}],
  "duration_math_needed": boolean,
  "duration_operation": "SUM" | "AVG" | "MAX" | null
}
"""
    STATIC_INSTRUCTIONS = """
You are an expert data analyst. Convert the question into a valid SQLite query.
Use the PRIMARY KEY and FOREIGN KEY constraints in the SCHEMA to determine correct JOINs.

RULES:
- Use LEFT JOIN for funnel analysis.
- Use COUNT(DISTINCT ...) when chaining across fact tables.
- Is_Billable is integer 1 or 0. Use WHERE Is_Billable = 1.
- When filtering by name, JOIN the dimension table and filter on the name column.
- ALWAYS use COLLATE NOCASE for string comparisons: WHERE col = 'value' COLLATE NOCASE

DURATION MATH (CRITICAL SAFETY GUARD):
Never run SUBSTR on a raw column. Always guard against malformed/null data:
  SUM(CASE 
        WHEN col IS NOT NULL AND LENGTH(col)=8 THEN 
          (CAST(SUBSTR(col, 1, 2) AS INTEGER) * 3600 + CAST(SUBSTR(col, 4, 2) AS INTEGER) * 60 + CAST(SUBSTR(col, 7, 2) AS INTEGER))
        ELSE 0 
      END)

Provide Output strictly matching this JSON schema:
{
  "sql": "SELECT ...",
  "filters_applied": ["..."],
  "reason": "..."
}
"""
    VERIFIER_INSTRUCTIONS = """
Check if the generated SQL correctly answers the user's question.

Failure types to check:
A. SEMANTIC MISMATCH
B. WRONG TABLE
C. MISSING JOIN
D. WRONG FILTER VALUE
E. WRONG AGGREGATION
F. WRONG JOIN TYPE
G. INCOMPLETE ANSWER
H. UNGUARDED DURATION MATH - Ensure SUBSTR uses the CASE WHEN length=8 guard.
I. NON-EXISTENT COLUMN - Cross-check against SCHEMA provided.

Provide Output strictly matching this JSON schema:
{
  "verdict": "PASS" | "FAIL",
  "reason": "...",
  "fix_hint": "..." 
}
"""

class SemanticEngine:
    @staticmethod
    def route_query(question: str) -> dict:
        try:
            resp = _generate_content(f"{PromptManager.ROUTER_PROMPT}\n\nQUESTION: {question}", json_mode=True)
            return json.loads(resp.text)
        except Exception:
            return {"route": "SQL", "reason": "Fallback to SQL on error"}

    @staticmethod
    def kpi_rag_answer(question: str, kpi_collection) -> str:
        results = kpi_collection.query(query_texts=[question], n_results=3)
        docs = results['documents'][0] if results['documents'] else []
        context = "\n".join(docs)
        prompt = f"Using ONLY the following KPI definitions, answer the user's question clearly and concisely.\n\nKPI DEFINITIONS:\n{context}\n\nQUESTION: {question}"
        try:
            return _generate_content(prompt).text.strip()
        except Exception as exc:
            return str(exc)

    @staticmethod
    def plan_query(question: str, schema: str) -> dict:
        prompt = f"{PromptManager.PLANNER_PROMPT}\n\nSCHEMA:\n{schema}\n\nQUESTION: {question}"
        try:
            resp = _generate_content(prompt, json_mode=True)
            return json.loads(resp.text)
        except Exception:
            return {"complexity": "SIMPLE", "plan": [{"step": 1, "description": question, "tables": []}], "duration_math_needed": False, "duration_operation": None}

    @staticmethod
    def generate_sql(question: str, schema: str, examples: list, fix_hint: str = "", plan: dict = None) -> tuple:
        ex_block = "EXAMPLES:\n" + "".join([f'Example {i}: "{ex["question"]}"\nSQL: {ex["sql"]}\n\n' for i, ex in enumerate(examples, 1)]) if examples else ""
        plan_block = f"QUERY PLAN:\n" + "".join([f"  Step {s['step']}: {s['description']} ({', '.join(s.get('tables', []))})\n" for s in plan.get("plan", [])]) if plan and plan.get("plan") else ""
        q = question if not fix_hint else f"{question}\n\nPREVIOUS ATTEMPT FAILED. FIX REQUIRED: {fix_hint}"
        prompt = f"{PromptManager.STATIC_INSTRUCTIONS}\n\nSCHEMA:\n{schema}\n\n{plan_block}\n{ex_block}\nQUESTION: {q}"
        try:
            resp = _generate_content(prompt, json_mode=True)
            p = json.loads(resp.text)
            return p.get("sql"), p.get("filters_applied", []), p.get("reason")
        except Exception as exc:
            return None, [], str(exc)

    @staticmethod
    def verify_sql(question: str, sql: str, schema: str) -> dict:
        prompt = f"{PromptManager.VERIFIER_INSTRUCTIONS}\n\nSCHEMA:\n{schema}\n\nUSER QUESTION: {question}\n\nGENERATED SQL:\n{sql}"
        try:
            resp = _generate_content(prompt, json_mode=True)
            return json.loads(resp.text)
        except Exception:
            return {"verdict": "PASS", "reason": "Verifier unavailable; using generated SQL as-is.", "fix_hint": ""}

    @staticmethod
    def generate_nl_answer(question: str, df: pd.DataFrame) -> str:
        if df is None or df.empty: return "No data found."
        prompt = f'User asked: "{question}"\nData:\n{df.head(5).to_string(index=False)}\nWrite ONE concise sentence summarising the key insight. No SQL.'
        try:
            return _generate_content(prompt).text.strip()
        except Exception as exc:
            return str(exc)

# ══════════════════════════════════════════
# 6. PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════
class PipelineOrchestrator:
    @staticmethod
    def sanity_check(question: str, df: pd.DataFrame) -> dict:
        if df is None or df.empty: 
            return {"passed": False, "warning": "Query returned 0 rows. Check WHERE clauses for over-filtering."}
        
        ID_PREFIXES = ('CHANNEL_', 'USER_', 'LANG_', 'PLAT_', 'OTYPE_', 'ITYPE_', 'INVID_', 'OUTVID_', 'TEAM_', 'CLIENT_')
        for col in df.columns:
            if df[col].dtype == object and len(df) > 0:
                sample = str(df[col].iloc[0])
                if any(sample.startswith(prefix) for prefix in ID_PREFIXES):
                    return {"passed": False, "warning": f"Column '{col}' contains raw IDs (e.g. {sample}). A dimension JOIN is missing."}
            
        if len(df) > 20000:
            return {"passed": False, "warning": "Result exceeds 20,000 rows. Probable Cartesian JOIN. Add GROUP BY or check JOIN conditions."}
            
        return {"passed": True, "warning": None}

    @staticmethod
    def run_pipeline(question: str, schema: str, examples: list) -> dict:
        t_plan = time.perf_counter()
        plan = SemanticEngine.plan_query(question, schema)
        plan_ms = round((time.perf_counter() - t_plan) * 1000, 1)

        conn, fix_hint, attempts = sqlite3.connect(Config.DB_PATH), "", []

        for attempt in range(1, Config.MAX_RETRIES + 2):
            t0 = time.perf_counter()
            sql, filters, reason = SemanticEngine.generate_sql(question, schema, examples, fix_hint, plan)
            gen_ms = round((time.perf_counter() - t0) * 1000, 1)

            if sql is None:
                attempts.append({"attempt": attempt, "sql": None, "verdict": "GENERATION_FAILED", "reason": reason, "gen_ms": gen_ms, "ver_ms": 0})
                break

            t1 = time.perf_counter()
            ver_result = SemanticEngine.verify_sql(question, sql, schema)
            ver_ms = round((time.perf_counter() - t1) * 1000, 1)

            attempts.append({
                "attempt": attempt, "sql": sql, "verdict": ver_result.get("verdict"),
                "reason": ver_result.get("reason"), "fix_hint": ver_result.get("fix_hint", ""),
                "gen_ms": gen_ms, "ver_ms": ver_ms,
            })

            if ver_result.get("verdict") == "PASS":
                try:
                    df = pd.read_sql_query(sql, conn)
                    sanity = PipelineOrchestrator.sanity_check(question, df)
                except Exception as e:
                    conn.close()
                    return {"sql": sql, "filters": filters, "df": None, "error": str(e), "verification": ver_result, "sanity": None, "plan": plan, "plan_ms": plan_ms, "attempts": attempts}
                conn.close()
                return {"sql": sql, "filters": filters, "df": df, "error": None, "verification": ver_result, "sanity": sanity, "plan": plan, "plan_ms": plan_ms, "attempts": attempts}
            else:
                fix_hint = ver_result.get("fix_hint", "")
                if attempt >= Config.MAX_RETRIES + 1:
                    try: df = pd.read_sql_query(sql, conn)
                    except Exception: df = None
                    conn.close()
                    return {"sql": sql, "filters": filters, "df": df, "error": None, "verification": ver_result, "sanity": {"passed": False, "warning": "Max retries reached."}, "plan": plan, "plan_ms": plan_ms, "attempts": attempts}

        conn.close()
        return {"sql": None, "filters": [], "df": None, "error": reason, "verification": None, "sanity": None, "plan": plan, "plan_ms": plan_ms, "attempts": attempts}

# ══════════════════════════════════════════
# 7. UI MANAGER
# ══════════════════════════════════════════
class UIManager:
    @staticmethod
    def smart_chart(df: pd.DataFrame):
        if df is None or df.empty or len(df.columns) < 2: return
        cols, numeric_cols = df.columns.tolist(), df.select_dtypes(include='number').columns.tolist()
        if not numeric_cols: return
        is_time = any(w in cols[0].lower() for w in ['month', 'year', 'date', 'week', 'quarter'])
        if is_time and len(df) >= 6: st.line_chart(data=df, x=cols[0], y=numeric_cols)
        elif len(numeric_cols) >= 2: st.bar_chart(data=df.set_index(cols[0])[numeric_cols])
        else: st.bar_chart(data=df, x=cols[0], y=numeric_cols[0])

    @staticmethod
    def render_app():
        st.set_page_config(page_title="Frammer AI Analytics", layout="wide")
        st.title("Frammer AI Analytics")
        st.caption("Pipeline: Schema RAG + Anchors → Query Planner → BM25 → SQL Gen → LLM Verifier → Sanity Check")

        if not os.path.exists(Config.DB_PATH):
            with st.spinner("Building database..."):
                DatabaseManager.initialize_database()
        else:
            DatabaseManager.initialize_database()

        examples = BM25Retriever.load_examples()
        schema_collection, kpi_collection = KnowledgeBase.setup_chroma_db()

        with st.sidebar:
            st.markdown("### Architecture Specs")
            st.markdown("**Router:** LLM Intent Engine (SQL vs KPI)")
            st.markdown("**Schema:** ChromaDB RAG + Fact Anchors")
            st.markdown("**Glossary:** ChromaDB Document RAG")
            st.markdown("**Constraint Engine:** Explicit PK/FK Injection")
            st.markdown("**Agent JSON:** Native `response_mime_type`")
            st.markdown("**Retrieval:** BM25 Mathematical Scoring")
            st.markdown("**Verification:** LLM-as-judge (9 Types)")
            st.markdown("**Sanity Check:** Execution-Aware Rules")
            st.markdown(f"**Max Retries:** {Config.MAX_RETRIES}")

            st.markdown("---")
            st.markdown("### 🧠 Add Custom KPI")
            with st.form("add_kpi_form"):
                new_kpi_name = st.text_input("KPI Name", placeholder="e.g. Mega Ratio")
                new_kpi_def = st.text_area("Definition", placeholder="e.g. Uploads divided by Active Channels")
                if st.form_submit_button("Save to Knowledge Base"):
                    if new_kpi_name.strip() and new_kpi_def.strip():
                        kpi_docs = [f"KPI Name: {new_kpi_name}\nDefinition: {new_kpi_def}"]
                        kpi_collection.upsert(documents=kpi_docs, ids=[new_kpi_name])
                        st.success(f"Added '{new_kpi_name}' to Vector DB!")
                    else:
                        st.warning("Please fill both fields.")

        user_input = st.text_input("Ask a question:", placeholder="e.g. Which team uploaded the most Hindi videos?")

        if st.button("Generate & Verify", type="primary"):
            if not user_input.strip():
                st.warning("Please enter a question.")
            else:
                with st.spinner("Executing Pipeline..."):
                    t_start = time.perf_counter()
                    
                    intent = SemanticEngine.route_query(user_input)
                    
                    if intent.get("route") == "KPI":
                        st.info(f"🛣️ **Routed to KPI Glossary** (Reason: {intent.get('reason')})")
                        answer = SemanticEngine.kpi_rag_answer(user_input, kpi_collection)
                        st.success(f"📖 {answer}")
                        st.caption(f"Total time: {round((time.perf_counter() - t_start) * 1000, 1)}ms")
                        st.stop()
                    
                    st.info(f"🛣️ **Routed to SQL Data Pipeline** (Reason: {intent.get('reason')})")
                    
                    retrieved = KnowledgeBase.retrieve_relevant_tables(user_input, schema_collection, k=Config.RAG_TOP_TABLES)
                    final_tables = KnowledgeBase.expand_schema_graph(retrieved)
                    
                    conn_check = sqlite3.connect(Config.DB_PATH)
                    filtered_schema = DatabaseManager.build_schema_with_pragma(conn_check, selected_tables=final_tables)
                    conn_check.close()
                    
                    selected = BM25Retriever.retrieve(user_input, examples)
                    result   = PipelineOrchestrator.run_pipeline(user_input, filtered_schema, selected)
                    total_ms = round((time.perf_counter() - t_start) * 1000, 1)

                st.caption(f"Total pipeline time: {total_ms}ms")

                with st.expander(f"Schema RAG — Context Engine loaded {len(final_tables)} tables"):
                    st.markdown(", ".join([f"`{t}`" for t in final_tables]))

                plan = result.get("plan", {})
                with st.expander(f"Query Plan — {plan.get('complexity', 'SIMPLE')}  |  {result.get('plan_ms', 0)}ms"):
                    for step in plan.get("plan", []):
                        st.markdown(f"**Step {step.get('step', '?')}:** {step.get('description', '')} ({', '.join(step.get('tables', []))})")

                with st.expander("Retrieved examples (BM25)"):
                    for ex in selected: st.markdown(f"- {ex['question']}")

                st.subheader("Pipeline trace")
                for a in result["attempts"]:
                    with st.expander(f"Attempt {a['attempt']} — {a.get('verdict', '?')}  |  gen: {a['gen_ms']}ms  |  verify: {a['ver_ms']}ms"):
                        st.markdown(f"**Verifier reason:** {a.get('reason', '—')}")
                        if a.get("fix_hint"): st.markdown(f"**Fix hint:** {a['fix_hint']}")
                        if a.get("sql"): st.code(a["sql"], language="sql")

                ver = result.get("verification")
                if ver:
                    if ver.get("verdict") == "PASS": st.success(f"Verification passed — {ver.get('reason')}")
                    else: st.warning(f"Verification failed after retries — {ver.get('reason')}")

                sanity = result.get("sanity")
                if sanity and not sanity.get("passed"): st.warning(f"Sanity check: {sanity.get('warning')}")

                df = result.get("df")
                if result.get("error") and df is None:
                    st.error(f"Error: {result['error']}")
                elif df is not None and not df.empty:
                    st.success(f"💡 {SemanticEngine.generate_nl_answer(user_input, df)}")
                    st.dataframe(df, use_container_width=True)
                    UIManager.smart_chart(df)
                elif df is not None and df.empty:
                    st.warning("Query returned no results.")

if __name__ == "__main__":
    UIManager.render_app()
