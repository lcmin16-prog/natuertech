# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    import streamlit as st
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Streamlit이 필요합니다.\n"
        "설치: `pip install streamlit pandas openpyxl plotly PyGithub google-generativeai`\n"
        "실행: `streamlit run app.py`\n"
        f"(원인: {exc})"
    ) from exc


# cache decorator compatibility (older Streamlit)
_cache_data = getattr(st, "cache_data", None) or st.cache  # type: ignore[attr-defined]


APP_TITLE = "포장재 리스크 관리 대시보드"
SHEET_NAME = "포장재 발주관리"

ABS_LOCAL_EXCEL_PATH = (
    r"C:\Users\vd1p2\바탕 화면\생산기획팀_이창민\2. 참고자료\생산대시보드\새 폴더 (2)\새 폴더\수주제품_대응_포장재_KPI_관리본.xlsx"
)
RELATIVE_EXCEL_PATH = str(Path("data") / "data.xlsx")  # for cloud/repo deployment (cross-platform)

DEFAULT_LOCAL_EXCEL_PATH = RELATIVE_EXCEL_PATH if Path(RELATIVE_EXCEL_PATH).exists() else ABS_LOCAL_EXCEL_PATH

INDEX_COLS = ["수주일", "제품코드", "제품명", "수주수량"]
DETAIL_COLS = [
    "품목코드",
    "품목명",
    "필요수량",
    "재고",
    "단가",
    "공급처",
    "발주일",
    "입고예정",
    "입고일자",
    "비고",
]


@dataclass(frozen=True)
class GitHubConfig:
    token: str
    repo_full_name: str  # org/repo
    repo_path: str  # path in repo (e.g. data/data.xlsx)
    branch: str | None = None


def _normalize_col(col: Any) -> str:
    if col is None:
        return ""
    s = str(col)
    # remove invisible / zero-width chars often found in Excel exports
    s = s.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    s = s.replace("\xa0", " ")  # non-breaking space
    s = s.replace("\n", " ").replace("\r", " ")
    return " ".join(s.split()).strip()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_col(c) for c in df.columns]
    # extra safety: strip any residual whitespace
    df.columns = pd.Index([str(c).strip() for c in df.columns])
    return df


def clean_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).replace({"nan": "", "None": "", "NaT": ""})
    s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def clean_date(series: pd.Series) -> pd.Series:
    s = series.copy().replace("-", pd.NA)
    return pd.to_datetime(s, errors="coerce")


def ensure_required_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [c for c in required if c not in df.columns]


def resolve_excel_path(path_str: str) -> Path:
    p = Path(path_str)
    if p.exists():
        return p
    rel = Path(RELATIVE_EXCEL_PATH)
    if rel.exists():
        return rel
    fallback = Path(__file__).resolve().parent / p.name
    if fallback.exists():
        return fallback
    return p


@_cache_data(show_spinner=False)
def load_packaging_excel(path_str: str, sheet_name: str) -> pd.DataFrame:
    path = resolve_excel_path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"엑셀 파일을 찾을 수 없습니다: {path}")
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    return normalize_columns(df)


def transform_packaging_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(raw_df).copy()

    for c in INDEX_COLS:
        if c in df.columns:
            df[c] = df[c].ffill()

    for c in ["수주수량", "필요수량", "재고", "발주수량", "단가"]:
        if c in df.columns:
            df[c] = clean_numeric(df[c])
            df[c] = df[c].round(0).astype("Int64")

    for c in ["수주일", "발주일", "입고예정", "입고일자", "디자인 회신일"]:
        if c in df.columns:
            df[c] = clean_date(df[c])

    if "발주일" in df.columns or "수주일" in df.columns:
        df["기준일"] = df.get("발주일")
        if "수주일" in df.columns:
            df["기준일"] = df["기준일"].fillna(df["수주일"])

    if "품목코드" in df.columns and "품목명" in df.columns:
        df = df[~(df["품목코드"].isna() & df["품목명"].isna())].copy()

    return df


def compute_risk_grades(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["단가_expected"] = pd.NA
    df["단가_volatility"] = 0.0

    if "품목코드" in df.columns and "단가" in df.columns:
        if "기준일" in df.columns:
            df = df.sort_values(["기준일"]).copy()

        def _calc(g: pd.DataFrame) -> pd.DataFrame:
            price = g["단가"].astype(float)
            expected = price.shift(1).rolling(3, min_periods=1).mean()
            vol = ((price - expected).abs() / expected.replace(0, pd.NA)).fillna(0.0)
            g["단가_expected"] = expected
            g["단가_volatility"] = vol
            return g

        df = df.groupby("품목코드", dropna=False, group_keys=False).apply(_calc)

    need = df.get("필요수량")
    stock = df.get("재고")
    vol = df.get("단가_volatility")

    grades: list[str] = []
    reasons: list[str] = []
    for i in range(len(df)):
        need_v = float(need.iat[i]) if need is not None and pd.notna(need.iat[i]) else None
        stock_v = float(stock.iat[i]) if stock is not None and pd.notna(stock.iat[i]) else None
        vol_v = float(vol.iat[i]) if vol is not None and pd.notna(vol.iat[i]) else 0.0

        r: list[str] = []
        if need_v is not None and stock_v is not None and stock_v < need_v:
            r.append("재고<필요수량")
        if vol_v > 0.10:
            r.append("단가변동>10%")

        if r:
            grades.append("A")
            reasons.append(", ".join(r))
        elif need_v is not None and stock_v is not None and need_v > 0 and need_v <= stock_v <= 1.2 * need_v:
            grades.append("B")
            reasons.append("재고 120% 이내")
        else:
            grades.append("C")
            reasons.append("재고 안정" if (need_v is not None and stock_v is not None) else "정보 부족")

    df["Risk"] = grades
    df["RiskReason"] = reasons
    if "필요수량" in df.columns and "재고" in df.columns:
        df["StockOut"] = (df["재고"] < df["필요수량"]).fillna(False)
    else:
        df["StockOut"] = False
    return df


def compute_kpis(df: pd.DataFrame) -> dict[str, str]:
    total_items = int(len(df))
    high_risk = int((df.get("Risk") == "A").sum()) if "Risk" in df.columns else 0
    stock_out = int(df.get("StockOut", False).sum())

    avg_trend_text = "-"
    if "기준일" in df.columns and "단가" in df.columns:
        d = df.dropna(subset=["기준일", "단가"]).copy()
        if not d.empty:
            last_date = pd.to_datetime(d["기준일"].max())
            recent = d[d["기준일"] >= last_date - pd.Timedelta(days=30)]["단가"].mean()
            prev = d[
                (d["기준일"] < last_date - pd.Timedelta(days=30))
                & (d["기준일"] >= last_date - pd.Timedelta(days=60))
            ]["단가"].mean()
            if pd.notna(recent) and pd.notna(prev) and prev != 0:
                avg_trend_text = f"{(recent - prev) / prev * 100:+.1f}%"

    return {
        "Total Items": f"{total_items:,}",
        "High Risk (A)": f"{high_risk:,}",
        "Stock-out": f"{stock_out:,}",
        "Avg Price Trend": avg_trend_text,
    }


def github_upsert_excel(content_bytes: bytes, cfg: GitHubConfig) -> tuple[bool, str]:
    try:
        from github import Github
        from github.GithubException import GithubException
    except Exception:
        return False, "PyGithub 미설치: `pip install PyGithub`"

    if not cfg.token or not cfg.repo_full_name or not cfg.repo_path:
        return False, "GitHub 설정(토큰/레포/경로)이 비어 있습니다."

    gh = Github(cfg.token)
    repo = gh.get_repo(cfg.repo_full_name)
    b64_content = base64.b64encode(content_bytes).decode("utf-8")

    sha: str | None = None
    try:
        existing = repo.get_contents(cfg.repo_path, ref=cfg.branch) if cfg.branch else repo.get_contents(cfg.repo_path)
        sha = getattr(existing, "sha", None)
    except GithubException as e:
        if getattr(e, "status", None) != 404:
            return False, f"GitHub 조회 실패: {e}"

    payload: dict[str, Any] = {
        "message": f"Update Excel: {Path(cfg.repo_path).name} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})",
        "content": b64_content,
    }
    if sha:
        payload["sha"] = sha
    if cfg.branch:
        payload["branch"] = cfg.branch

    try:
        repo._requester.requestJsonAndCheck(  # type: ignore[attr-defined]
            "PUT",
            f"/repos/{cfg.repo_full_name}/contents/{cfg.repo_path}",
            input=payload,
        )
        return True, "GitHub 업로드 완료"
    except Exception as e:
        return False, f"GitHub 업로드 실패: {e}"


def gemini_generate_scenarios(rows: pd.DataFrame) -> tuple[bool, str]:
    try:
        import google.generativeai as genai
    except Exception:
        return False, "Gemini SDK 미설치: `pip install google-generativeai`"

    try:
        api_key = str(st.secrets["GEMINI_API_KEY"]).strip()
    except Exception as e:
        return (
            False,
            "GEMINI_API_KEY가 설정되지 않았습니다.\n"
            "프로젝트 루트에 `.streamlit/secrets.toml` 파일을 만들고 아래처럼 설정하세요:\n"
            'GEMINI_API_KEY = \"YOUR_API_KEY\"\n'
            f"(원인: {e})",
        )

    keep_cols = [
        c
        for c in ["품목코드", "품목명", "공급처", "필요수량", "재고", "단가", "비고", "발주일", "입고예정", "입고일자", "Risk", "RiskReason"]
        if c in rows.columns
    ]
    payload = rows[keep_cols].copy().fillna("")
    payload = payload.head(20)

    instructions = (
        "당신은 SCM 포장재 리스크 관리 전문가입니다.\n"
        "아래 데이터(JSON 배열)를 보고 각 품목별 Actionable Scenarios를 2~4개씩 한국어로 제시하세요.\n"
        "형식:\n"
        "- [품목코드] 품목명: 시나리오(원인) / 영향 / 권장 조치(수량 또는 % 포함)\n"
        "단가와 비고를 적극 활용하고, 재고<필요수량 또는 단가 급변 시 우선순위를 높이세요.\n"
        "추정은 추정이라고 명시하고 과장/확정 표현은 피하세요."
    )

    genai.configure(api_key=api_key)
    prompt = instructions + "\n\nDATA(JSON):\n" + payload.to_json(orient="records", force_ascii=False)

    errors: list[str] = []
    for model_name in ["gemini-1.5-flash", "gemini-1.5-flash-latest"]:
        try:
            model = genai.GenerativeModel(model_name)
            resp = model.generate_content(prompt)
            return True, getattr(resp, "text", None) or str(resp)
        except Exception as me:
            errors.append(f"{model_name}: {me}")

    try:
        models = list(genai.list_models())

        def supports_generate(m: Any) -> bool:
            methods = getattr(m, "supported_generation_methods", None) or []
            return ("generateContent" in methods) or ("generate_content" in methods)

        flash_models = [m for m in models if "flash" in str(getattr(m, "name", "")).lower() and supports_generate(m)]
        if not flash_models:
            flash_models = [m for m in models if "flash" in str(getattr(m, "name", "")).lower()]

        pick = flash_models[0] if flash_models else (models[0] if models else None)
        if pick is None:
            return False, "Gemini 사용 가능 모델을 찾지 못했습니다."

        picked_name = str(getattr(pick, "name", "")).strip()
        candidates = [picked_name]
        if picked_name.startswith("models/"):
            candidates.append(picked_name.split("/", 1)[1])

        last_err: Exception | None = None
        for cand in candidates:
            try:
                model = genai.GenerativeModel(cand)
                resp = model.generate_content(prompt)
                return True, getattr(resp, "text", None) or str(resp)
            except Exception as ce:
                last_err = ce

        return False, f"Gemini 모델 폴백 실패: {last_err} (시도: {', '.join(candidates)})"
    except Exception as le:
        return False, "Gemini 모델 폴백 실패(모델 목록 조회 실패). " + " / ".join(errors + [str(le)])


def format_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def fmt_int(x: Any) -> str:
        if x is None or pd.isna(x):
            return "-"
        try:
            return f"{int(x):,}"
        except Exception:
            return str(x)

    def fmt_date(x: Any) -> str:
        if x is None or pd.isna(x):
            return "-"
        try:
            dt = pd.to_datetime(x, errors="coerce")
            if dt is None or pd.isna(dt):
                return "-"
            return dt.strftime("%Y-%m-%d")
        except Exception:
            s = str(x)
            return s[:10] if s else "-"

    for c in ["수주수량", "필요수량", "재고", "발주수량", "단가"]:
        if c in out.columns:
            out[c] = out[c].map(fmt_int)

    if "단가_volatility" in out.columns:
        def fmt_pct(x: Any) -> str:
            if x is None or pd.isna(x):
                return "-"
            try:
                return f"{float(x) * 100:.1f}%"
            except Exception:
                return str(x)

        out["단가_volatility"] = out["단가_volatility"].map(fmt_pct)

    for c in ["수주일", "발주일", "입고예정", "입고일자", "디자인 회신일", "기준일"]:
        if c in out.columns:
            out[c] = out[c].map(fmt_date)

    out = out.fillna("-")
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].replace({"": "-", "nan": "-", "NaT": "-"})
    return out


def style_risk_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def risk_cell(val: Any) -> str:
        v = str(val).strip().upper()
        if v == "A":
            return "background-color: rgba(255,77,79,0.25); color: #ffffff; font-weight: 800;"
        if v == "B":
            return "background-color: rgba(250,173,20,0.20); color: #ffffff; font-weight: 800;"
        if v == "C":
            return "background-color: rgba(82,196,26,0.18); color: #ffffff; font-weight: 800;"
        return ""

    def row_bg(row: pd.Series) -> list[str]:
        if str(row.get("Risk", "")).strip().upper() == "A":
            return ["background-color: rgba(255,77,79,0.14);"] * len(row)
        return [""] * len(row)

    styler = df.style.apply(row_bg, axis=1)
    if "Risk" in df.columns:
        # pandas 2.1+: Styler.map (applymap deprecated)
        if hasattr(styler, "map"):
            styler = styler.map(risk_cell, subset=["Risk"])
        else:
            styler = styler.applymap(risk_cell, subset=["Risk"])

    numeric_cols = [c for c in ["수주수량", "필요수량", "재고", "발주수량", "단가", "단가_volatility"] if c in df.columns]
    if numeric_cols:
        styler = styler.set_properties(subset=numeric_cols, **{"text-align": "right"})
    return styler


def inject_css() -> None:
    st.markdown(
        """
        <style>
          .stApp { background: #0b1220; color: #e6edf3; }
          [data-testid="stSidebar"] { background: #070d19; }
          [data-testid="stSidebar"] * { color: #FFFFFF !important; }
          [data-testid="stSidebar"] input, [data-testid="stSidebar"] textarea { color: #FFFFFF !important; }
          .kpi-card {
            background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 14px;
            padding: 14px 16px;
          }
          .kpi-title { font-size: 12px; color: rgba(230,237,243,0.75); margin-bottom: 6px; }
          .kpi-value { font-size: 26px; font-weight: 700; line-height: 1.1; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_charts(df: pd.DataFrame) -> None:
    try:
        import plotly.express as px
    except Exception:
        st.info("Plotly 미설치: `pip install plotly`")
        return

    st.subheader("트렌드 / 현황")
    left, right = st.columns([2, 1])

    with left:
        if "기준일" in df.columns and "단가" in df.columns:
            d = df.dropna(subset=["기준일", "단가"]).copy()
            if not d.empty:
                d["월"] = d["기준일"].dt.to_period("M").dt.to_timestamp()
                grp = ["월"] + (["품목명"] if "품목명" in d.columns else [])
                agg = d.groupby(grp, as_index=False)["단가"].mean()
                fig = px.line(agg, x="월", y="단가", color="품목명" if "품목명" in agg.columns else None, title="단가 추이(월 평균)")
                fig.update_layout(template="plotly_dark", height=380, margin=dict(l=10, r=10, t=45, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("단가 데이터가 없습니다.")
        else:
            st.caption("기준일/단가 컬럼이 없어 추이를 표시할 수 없습니다.")

    with right:
        if {"품목명", "재고", "필요수량"}.issubset(df.columns):
            d = df.copy()
            d["부족량"] = (d["필요수량"] - d["재고"]).clip(lower=0)
            d = d.sort_values(["부족량"], ascending=False).head(15)
            long = d.melt(id_vars=["품목명"], value_vars=["재고", "필요수량"], var_name="구분", value_name="수량")
            fig2 = px.bar(
                long,
                y="품목명",
                x="수량",
                color="구분",
                barmode="group",
                orientation="h",
                title="재고 vs 필요수량 (Top 15 부족)",
            )
            fig2.update_layout(template="plotly_dark", height=380, margin=dict(l=10, r=10, t=45, b=10))
            fig2.update_yaxes(autorange="reversed")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.caption("품목명/재고/필요수량 컬럼이 없어 재고 차트를 표시할 수 없습니다.")


@_cache_data(show_spinner=False)
def make_excel_bytes(df: pd.DataFrame, sheet_name: str = "RiskReport") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return out.getvalue()


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    inject_css()

    st.subheader("🌍 글로벌 원가 영향 변수 (Market Intelligence)")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("국제 유가 (WTI)", "$82.45", "▲ 1.2%")
    with m2:
        st.metric("환율 (USD/KRW)", "1,354.2원", "▼ 0.5%", delta_color="inverse")
    with m3:
        st.metric("해상운임 (SCFI)", "2,150.3", "▲ 15.2")
    with m4:
        st.metric("공급망 리스크", "주의", "중동 정세")

    st.title(APP_TITLE)
    st.caption("Local Excel 동기화 + 리스크 등급화 + (옵션) GitHub 업로드 / Gemini 시나리오 생성")

    st.sidebar.header("설정")
    path_input = st.sidebar.text_input("Local/Relative Excel Path", value=DEFAULT_LOCAL_EXCEL_PATH)
    st.sidebar.caption(f"클라우드용 상대경로 예시: `{RELATIVE_EXCEL_PATH}`")

    st.sidebar.subheader("업로드 / 동기화")
    uploaded = st.sidebar.file_uploader("새 Excel 업로드(.xlsx)", type=["xlsx"])
    save_local = st.sidebar.checkbox("업로드 파일을 로컬 경로에 저장", value=True)

    st.sidebar.subheader("GitHub Sync (옵션)")
    github_enabled = st.sidebar.checkbox("업로드 시 GitHub에 푸시", value=True)
    gh_repo = st.sidebar.text_input("Repo (org/repo)", value="")
    gh_branch = st.sidebar.text_input("Branch (선택)", value="")
    gh_repo_path = st.sidebar.text_input("Repo file path", value=f"data/{Path(path_input).name}")
    gh_token = st.sidebar.text_input("GitHub Token", value="", type="password")

    st.sidebar.subheader("Gemini Insight (옵션)")
    gemini_enabled = st.sidebar.checkbox("리스크 시나리오 생성 사용", value=False)
    st.sidebar.caption("API Key는 `st.secrets['GEMINI_API_KEY']`에서만 읽습니다.")

    if uploaded is not None:
        content = uploaded.getvalue()
        st.sidebar.success(f"업로드 수신: {uploaded.name} ({len(content):,} bytes)")

        if save_local:
            try:
                target = Path(path_input)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
                st.sidebar.info(f"로컬 저장 완료: {target}")
                if hasattr(load_packaging_excel, "clear"):
                    load_packaging_excel.clear()  # type: ignore[attr-defined]
            except Exception as e:
                st.sidebar.error(f"로컬 저장 실패: {e}")

        if github_enabled:
            cfg = GitHubConfig(
                token=gh_token.strip(),
                repo_full_name=gh_repo.strip(),
                repo_path=gh_repo_path.strip().lstrip("/"),
                branch=gh_branch.strip() or None,
            )
            ok, msg = github_upsert_excel(content, cfg)
            (st.sidebar.success if ok else st.sidebar.error)(msg)

    df: pd.DataFrame | None = None
    load_error: str | None = None
    try:
        df_raw = load_packaging_excel(path_input, SHEET_NAME)
        df = compute_risk_grades(transform_packaging_df(df_raw))
    except Exception as e:
        load_error = str(e)

    if df is None:
        st.error("데이터를 불러올 수 없습니다.")
        st.write(load_error or "알 수 없는 오류")
        st.info("Excel 경로를 확인하거나, 좌측에서 새 Excel 파일을 업로드하세요.")
        return

    missing = ensure_required_columns(df, INDEX_COLS + DETAIL_COLS)
    if missing:
        st.warning(f"일부 필수 컬럼이 없습니다(가능한 범위에서 계속 진행): {', '.join(missing)}")

    st.subheader("필터")
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.1, 1.1])
    with c1:
        risk_filter = st.multiselect("Risk", options=["A", "B", "C"], default=["A", "B", "C"])
    with c2:
        supplier_options = (
            sorted([x for x in df.get("공급처", pd.Series([], dtype=object)).dropna().unique().tolist()])
            if "공급처" in df.columns
            else []
        )
        suppliers = st.multiselect("공급처", options=supplier_options, default=[])
    with c3:
        query = st.text_input("품목 검색(코드/명)", value="")
    with c4:
        max_rows = st.number_input("표 최대 행", min_value=50, max_value=2000, value=500, step=50)

    dff = df.copy()
    if "Risk" in dff.columns and risk_filter:
        dff = dff[dff["Risk"].isin(risk_filter)].copy()
    if suppliers and "공급처" in dff.columns:
        dff = dff[dff["공급처"].isin(suppliers)].copy()
    if query.strip():
        q = query.strip()
        hay = pd.Series([""] * len(dff), index=dff.index, dtype=str)
        for col in ["품목코드", "품목명", "제품코드", "제품명", "비고"]:
            if col in dff.columns:
                hay = hay + " " + dff[col].astype(str)
        dff = dff[hay.str.contains(q, case=False, na=False)].copy()
    dff = dff.head(int(max_rows)).copy()

    kpis = compute_kpis(df)
    k1, k2, k3, k4 = st.columns(4)
    for col, (title, value) in zip([k1, k2, k3, k4], kpis.items(), strict=False):
        with col:
            st.markdown(
                f"""
                <div class="kpi-card">
                  <div class="kpi-title">{title}</div>
                  <div class="kpi-value">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.subheader("발주관리 테이블")
    show_cols = [c for c in (INDEX_COLS + DETAIL_COLS + ["발주수량", "Risk", "RiskReason", "단가_volatility"]) if c in dff.columns]
    table_df = dff[show_cols].copy()
    display_df = format_table_for_display(table_df)

    column_config: dict[str, Any] = {
        "Risk": st.column_config.TextColumn("Risk Grade", help="A(적색)/B(황색)/C(녹색)") if "Risk" in display_df.columns else None
    }
    column_config = {k: v for k, v in column_config.items() if v is not None}
    for c in ["수주수량", "필요수량", "재고", "발주수량", "단가", "단가_volatility"]:
        if c in display_df.columns:
            column_config[c] = st.column_config.TextColumn(c)
    for c in ["수주일", "발주일", "입고예정", "입고일자", "디자인 회신일"]:
        if c in display_df.columns:
            column_config[c] = st.column_config.TextColumn(c)

    st.dataframe(
        style_risk_dataframe(display_df),
        use_container_width=True,
        height=420,
        column_config=column_config,
        hide_index=True,
    )

    report_name = f"risk_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.download_button(
        "Download Risk Report",
        data=make_excel_bytes(display_df),
        file_name=report_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    build_charts(dff)

    if gemini_enabled:
        st.subheader("AI 시나리오 (Gemini, 단가/비고 기반)")
        st.caption("주의: 민감정보가 포함될 수 있으니 전송 전에 검토하세요.")

        only_high_risk = st.checkbox("Grade A만 분석", value=True)
        target = dff.copy()
        if only_high_risk and "Risk" in target.columns:
            target = target[target["Risk"] == "A"].copy()

        n = st.slider("분석할 항목 수(상위)", min_value=1, max_value=20, value=8)
        if {"필요수량", "재고"}.issubset(target.columns):
            target = target.assign(부족량=(target["필요수량"] - target["재고"]).clip(lower=0)).sort_values("부족량", ascending=False)
        target = target.head(int(n))

        target_view = target[[c for c in ["품목코드", "품목명", "공급처", "필요수량", "재고", "단가", "비고", "Risk", "RiskReason", "발주일", "입고예정", "입고일자", "단가_volatility"] if c in target.columns]].copy()
        target_display = format_table_for_display(target_view)
        st.dataframe(style_risk_dataframe(target_display), use_container_width=True, height=220, hide_index=True)

        if st.button("Gemini로 Actionable Scenarios 생성", type="primary"):
            with st.spinner("Gemini 분석 중..."):
                ok, text = gemini_generate_scenarios(target)
            (st.success if ok else st.error)("완료" if ok else "실패")
            st.text_area("결과", value=text, height=280)


if __name__ == "__main__":
    main()
