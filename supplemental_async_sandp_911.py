import asyncio
import asyncpg
import csv
from scipy.stats import pearsonr
from postgres_config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

async def create_db_pool():
    return await asyncpg.create_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        min_size=1,  # Minimum number of connections in the pool
        max_size=20  # Maximum number of connections in the pool; adjust as needed
    )

async def fetch_typ_desc_values(pool):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT typ_desc FROM sch_nypd_calls_tables.types_of_calls")

async def fetch_data_for_typ_desc(pool, typ_desc):
    async with pool.acquire() as conn:
        query = """
        SELECT
            COUNT(*) as count,
            ts5.percent_change
        FROM 
            sch_nypd_calls_tables.tb_call_data tcb
        RIGHT JOIN
            (
                SELECT
                    date,
                    (close - LAG(close, 252) OVER (ORDER BY date)) / LAG(close, 252) OVER (ORDER BY date) * 100 AS percent_change
                FROM 
                    sch_nypd_calls_tables.tb_sp_500
            ) ts5
        ON 
            DATE(tcb.incident_date) = ts5.date
        WHERE tcb.typ_desc = $1
        GROUP BY ts5.date, ts5.percent_change
        """
        return await conn.fetch(query, typ_desc)

async def main():
    pool = await create_db_pool()

    typ_desc_values = await fetch_typ_desc_values(pool)

    data = {}
    tasks = [fetch_data_for_typ_desc(pool, typ_desc[0]) for typ_desc in typ_desc_values]
    results = await asyncio.gather(*tasks)

    for typ_desc, result in zip(typ_desc_values, results):
        data[typ_desc[0]] = {'counts': [], 'percent_changes': []}
        for count, percent_change in result:
            if percent_change is not None:
                # Convert both count and percent_change to float
                data[typ_desc[0]]['counts'].append(float(count))
                data[typ_desc[0]]['percent_changes'].append(float(percent_change))

    # Calculate and sort correlations
    correlations = []
    for typ_desc, values in data.items():
        if len(values['counts']) > 1 and len(values['percent_changes']) > 1:
            corr, _ = pearsonr(values['counts'], values['percent_changes'])
            correlations.append((typ_desc, corr))
            
    
    correlations.sort(key=lambda x: x[1], reverse=True)

    # Save to CSV file
    with open('correlation_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Type Description', 'Correlation Coefficient'])
        for typ_desc, corr in correlations:
            csvwriter.writerow([typ_desc, corr])

    # Close the pool
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
