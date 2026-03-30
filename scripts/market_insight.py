import duckdb

print("ANALYZING IT MARKET FROM SILVER LAYER...")
conn = duckdb.connect('../job_market.duckdb')

# 1. Analyst demand by level
print("\n1. DEMAND BY LEVEL:")
level_stats = conn.execute("""
    SELECT 
        job_level as Level, 
        COUNT(*) as Total_Jobs
    FROM silver_itviec_jobs
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
    FROM silver_itviec_jobs
    GROUP BY ai_job_role
    ORDER BY Total_Jobs DESC
    LIMIT 10;
""").df()
print(role_stats.to_string(index=False))

# 3. Analyst top hottest tech stacks
print("\n3. TOP HOTTEST TECH STACKS:")
tech_stats = conn.execute("""
    -- Use UNNEST to split the "Python, AWS, SQL" string into separate rows
    SELECT TRIM(value) as Tech_Skill, COUNT(*) as Mentions
    FROM silver_itviec_jobs, 
    UNNEST(STRING_SPLIT(ai_core_tech_stack, ',')) as t(value)
    WHERE value != '' AND value != 'Unknown'
    GROUP BY Tech_Skill
    ORDER BY Mentions DESC
    LIMIT 10;
""").df()
print(tech_stats.to_string(index=False))

conn.close()