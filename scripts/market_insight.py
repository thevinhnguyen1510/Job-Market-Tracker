import os
import duckdb

print("ANALYZING IT MARKET FROM SILVER LAYER (ACTIVE JOBS ONLY)...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    # Fallback to local
    db_path = 'job_market.duckdb'

conn = duckdb.connect(db_path, read_only=True)

# 1. Analyst demand by level
print("\n1. DEMAND BY LEVEL:")
level_stats = conn.execute("""
    SELECT 
        job_level as Level, 
        COUNT(*) as Total_Jobs
    FROM silver_jobs
    WHERE status = 'Active' AND job_level NOT IN ('Unknown', 'Error')
    GROUP BY job_level
    ORDER BY Total_Jobs DESC;
""").df()
print(level_stats.to_string(index=False))

# 2. Analyst top hottest roles
print("\n2. TOP HOTTEST ROLES:")
role_stats = conn.execute("""
    SELECT 
        ai_job_role as Role, 
        COUNT(*) as Total_Jobs
    FROM silver_jobs
    WHERE status = 'Active' AND ai_job_role NOT IN ('Unknown', 'Error')
    GROUP BY ai_job_role
    ORDER BY Total_Jobs DESC
    LIMIT 10;
""").df()
print(role_stats.to_string(index=False))

# 3. Analyst top hottest tech stacks
print("\n3. TOP HOTTEST TECH STACKS:")
tech_stats = conn.execute("""
    SELECT 
        skill_name as Tech_Skill,
        skill_category,
        COUNT(DISTINCT job_id) as Mentions
    FROM silver_job_skills
    WHERE status = 'Active'
    GROUP BY skill_name, skill_category
    ORDER BY Mentions DESC
    LIMIT 10;
""").df()
print(tech_stats.to_string(index=False))

conn.close()