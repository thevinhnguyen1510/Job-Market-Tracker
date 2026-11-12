"""Render market metrics from the warehouse."""

import duckdb
import plotly.express as px
import streamlit as st

from job_market.config import Settings


def render(settings: Settings) -> None:
    """Read warehouse metrics and render the filterable market dashboard."""
    conn = None
    st.markdown("### 📊 IT Market Intelligence Dashboard")
    st.markdown("Data is automatically extracted, standardized, and visualized directly from the **Silver Data Layer** (ITviec & TopCV).")

    try:
        conn = duckdb.connect(str(settings.duckdb_path), read_only=True)
        st.markdown("#### 🌍 1. Macro Market Overview (Market Structure)")

        macro_jobs = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE job_level != 'Error' AND status = 'Active'").fetchone()[0]
        macro_yoe = conn.execute("SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_all_jobs WHERE min_years_of_experience IS NOT NULL AND status = 'Active'").fetchone()[0]
        macro_yoe = macro_yoe if macro_yoe is not None else 0

        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Total Jobs Scanned", f"{macro_jobs:,}")
        col_m2.metric("Market Avg. Experience", f"{macro_yoe} Yrs")
        col_m3.metric("Data Sources", "ITviec & TopCV")

        macro_col1, macro_col2 = st.columns(2)

        with macro_col1:
            df_levels = conn.execute("""
                SELECT job_level, COUNT(*) as count
                FROM silver_all_jobs
                WHERE job_level != 'Error' AND job_level IS NOT NULL AND status = 'Active'
                GROUP BY job_level
                ORDER BY count DESC
            """).df()
            if not df_levels.empty:
                fig_donut = px.pie(
                    df_levels, values='count', names='job_level', hole=0.4,
                    title="Market Structure: Job Levels",
                    color_discrete_sequence=px.colors.sequential.Teal
                )
                fig_donut.update_traces(textposition='inside', textinfo='percent+label')
                fig_donut.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_donut, width="stretch")

        with macro_col2:
            df_yoe_macro = conn.execute("""
                SELECT min_years_of_experience
                FROM silver_all_jobs
                WHERE min_years_of_experience IS NOT NULL AND status = 'Active'
            """).df()
            if not df_yoe_macro.empty:
                fig_hist = px.histogram(
                    df_yoe_macro, x="min_years_of_experience", nbins=10,
                    title="Market Structure: Required Experience",
                    color_discrete_sequence=['#ff4b4b']
                )
                fig_hist.update_layout(xaxis_title="Years of Experience", yaxis_title="Number of Jobs", margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_hist, width="stretch")

        st.divider()
        st.markdown("#### 🔬 2. Role-Specific Deep Dive (Filterable)")

        raw_sources = conn.execute("SELECT DISTINCT source FROM silver_all_jobs WHERE source IS NOT NULL AND source != '' ORDER BY source").df()['source'].tolist()
        db_levels = conn.execute("SELECT DISTINCT job_level FROM silver_all_jobs WHERE job_level != 'Error' AND job_level IS NOT NULL AND job_level != ''").df()['job_level'].tolist()

        opt_sources = tuple(["All Sources"] + raw_sources)

        hr_order = ["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director"]
        valid_db_levels = [lvl for lvl in hr_order if lvl in db_levels]
        extra_levels = [lvl for lvl in db_levels if lvl not in hr_order]

        opt_levels = ["All Levels"] + valid_db_levels + extra_levels

        def enforce_level_selection():
            """Restore the default filter when the level selection is cleared."""
            if st.session_state.filter_level_pill is None:
                st.session_state.filter_level_pill = "All Levels"

        if "filter_level_pill" not in st.session_state:
            st.session_state.filter_level_pill = "All Levels"

        st.markdown("##### 🔍 Filter Data")

        selected_source = st.selectbox("🌐 Select Data Source:", options=opt_sources)

        selected_level = st.pills(
            "🎯 Select Job Level:",
            options=opt_levels,
            key="filter_level_pill",
            on_change=enforce_level_selection
        )

        if not selected_level:
            selected_level = "All Levels"

        filters = ["status = 'Active'"]
        if selected_source != "All Sources":
            filters.append(f"source = '{selected_source}'")
        if selected_level != "All Levels":
            filters.append(f"job_level = '{selected_level}'")

        global_filter_sql = ""
        if filters:
            global_filter_sql = "AND " + " AND ".join(filters)

        dyn_jobs = conn.execute(f"SELECT COUNT(*) FROM silver_all_jobs WHERE job_level != 'Error' {global_filter_sql}").fetchone()[0]
        dyn_yoe = conn.execute(f"SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_all_jobs WHERE min_years_of_experience IS NOT NULL {global_filter_sql}").fetchone()[0]
        dyn_yoe = dyn_yoe if dyn_yoe is not None else 0

        eng_demand = conn.execute(f"SELECT COUNT(*) FROM silver_all_jobs WHERE english_requirement != 'Not mentioned' AND english_requirement IS NOT NULL {global_filter_sql}").fetchone()[0]
        eng_pct = round((eng_demand / dyn_jobs) * 100, 1) if dyn_jobs > 0 else 0

        col_d1, col_d2, col_d3 = st.columns(3)
        col_d1.metric(f"Filtered Jobs", f"{dyn_jobs:,}")
        col_d2.metric("Avg. Experience", f"{dyn_yoe} Yrs")
        col_d3.metric("English Required", f"{eng_pct}%", help="Percentage of JDs requiring English (from Intermediate level up)")

        dyn_col1, dyn_col2 = st.columns([1.5, 1])

        with dyn_col1:
            sql_tech_stack = f"""
                WITH unnested_skills AS (
                    SELECT
                        UNNEST(from_json(ai_core_tech_stack, '["VARCHAR"]')) AS skill
                    FROM silver_all_jobs
                    WHERE ai_core_tech_stack IS NOT NULL
                      AND ai_core_tech_stack != '[]'
                      AND job_level != 'Error'
                      {global_filter_sql}
                )
                SELECT skill, COUNT(*) as total_mentions
                FROM unnested_skills
                WHERE skill != '' AND skill IS NOT NULL
                GROUP BY skill
                ORDER BY total_mentions DESC, skill ASC
                LIMIT 10
            """

            df_skills = conn.execute(sql_tech_stack).df()

            if not df_skills.empty:
                fig_bar = px.bar(
                    df_skills, x='total_mentions', y='skill', orientation='h',
                    title=f"Top 10 Tech Stack",
                    color='total_mentions', color_continuous_scale='Reds'
                )
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_bar, width="stretch")
            else:
                st.info("No tech stack data available for this filter.")

        with dyn_col2:
            df_eng_levels = conn.execute(f"""
                SELECT english_requirement, COUNT(*) as count
                FROM silver_all_jobs
                WHERE english_requirement IS NOT NULL {global_filter_sql}
                GROUP BY english_requirement
                ORDER BY count DESC, english_requirement ASC
            """).df()

            if not df_eng_levels.empty:
                fig_eng = px.bar(
                    df_eng_levels, x='english_requirement', y='count',
                    title=f"English Proficiency Demand",
                    color='english_requirement',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_eng.update_layout(
                    xaxis_title="",
                    yaxis_title="Number of Jobs",
                    showlegend=False,
                    xaxis_tickangle=-45,
                    margin=dict(t=40, b=0, l=0, r=0)
                )
                st.plotly_chart(fig_eng, width="stretch")
        st.divider()
        st.markdown("#### 💰 3. Tech Salary Benchmark (Gold Data Mart)")
        st.markdown("Salary statistics across **Roles & Seniority Levels** automatically calculated from the **Gold Data Mart** (`gold_salary_benchmark`). Values displayed in **Million VND / month**.")
        sal_summary = conn.execute("""
            SELECT
                COUNT(*) as total_disclosed,
                ROUND(MEDIAN(salary_avg_vnd), 1) as overall_median,
                ROUND(AVG(salary_avg_vnd), 1) as overall_avg
            FROM silver_all_jobs
            WHERE salary_avg_vnd > 0 AND status = 'Active'
        """).fetchone()

        top_paying = conn.execute("""
            SELECT job_role, job_level, median_salary_vnd
            FROM gold_salary_benchmark
            WHERE median_salary_vnd > 0
            ORDER BY median_salary_vnd DESC
            LIMIT 1
        """).fetchone()

        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("Disclosed Salary Jobs", f"{sal_summary[0]:,}")
        col_s2.metric("Market Median Tech Salary", f"{sal_summary[1]}M VND/mo")
        if top_paying:
            col_s3.metric("Top Paying Role & Level", f"{top_paying[0]} ({top_paying[1]})", f"{top_paying[2]}M VND median")
        else:
            col_s3.metric("Top Paying Role", "N/A")
        df_salary_bench = conn.execute("""
            SELECT
                job_role,
                job_level,
                total_salary_disclosed_jobs,
                min_salary_vnd,
                p25_salary_vnd,
                median_salary_vnd,
                p75_salary_vnd,
                max_salary_vnd,
                avg_salary_vnd
            FROM gold_salary_benchmark
            WHERE median_salary_vnd > 0
            ORDER BY job_role,
                CASE job_level
                    WHEN 'Intern' THEN 1
                    WHEN 'Fresher' THEN 2
                    WHEN 'Junior' THEN 3
                    WHEN 'Middle' THEN 4
                    WHEN 'Senior' THEN 5
                    WHEN 'Manager' THEN 6
                    WHEN 'Director' THEN 7
                    ELSE 8
                END
        """).df()

        if not df_salary_bench.empty:
            all_roles = sorted(df_salary_bench['job_role'].unique().tolist())
            selected_roles = st.multiselect(
                "🎯 Filter by Job Role:",
                options=all_roles,
                default=[r for r in ["Data Engineer", "Data/Business Analyst", "AI/Machine Learning", "Backend"] if r in all_roles] or all_roles[:4]
            )

            if selected_roles:
                df_salary_filtered = df_salary_bench[df_salary_bench['job_role'].isin(selected_roles)]
            else:
                df_salary_filtered = df_salary_bench

            fig_salary = px.bar(
                df_salary_filtered,
                x='job_role',
                y='median_salary_vnd',
                color='job_level',
                barmode='group',
                title="Median Monthly Salary by Role & Seniority Level (Million VND)",
                labels={'median_salary_vnd': 'Median Salary (M VND)', 'job_role': 'Job Role', 'job_level': 'Level'},
                category_orders={"job_level": ["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director"]},
                text_auto='.1f',
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_salary.update_layout(
                xaxis_title="",
                yaxis_title="Million VND / Month",
                legend_title="Level",
                margin=dict(t=40, b=0, l=0, r=0)
            )
            st.plotly_chart(fig_salary, width="stretch")

            with st.expander("📋 View Detailed Salary Distribution (P25 - Median - P75 - Min/Max)"):
                display_cols = {
                    'job_role': 'Role',
                    'job_level': 'Level',
                    'total_salary_disclosed_jobs': 'Disclosed Jobs',
                    'min_salary_vnd': 'Min (M)',
                    'p25_salary_vnd': 'P25 (M)',
                    'median_salary_vnd': 'Median (M)',
                    'p75_salary_vnd': 'P75 (M)',
                    'max_salary_vnd': 'Max (M)',
                    'avg_salary_vnd': 'Average (M)'
                }
                st.dataframe(
                    df_salary_filtered.rename(columns=display_cols),
                    width="stretch",
                    hide_index=True
                )
        else:
            st.info("Insufficient disclosed salary data to display distribution chart.")

    except Exception as e:
        st.error(f"Dashboard query error: {e}")
    finally:
        if conn is not None:
            conn.close()
