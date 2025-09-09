import os
import asyncio
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
import asyncpg

app = FastAPI(title="RentUZ MVP", version="0.1.0")

# Render обычно отдаёт DSN как postgres://..., asyncpg нормально ест и postgres:// и postgresql://
DB_URL = os.getenv("DATABASE_URL")  # добавим на Шаге 3

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.on_event("startup")
async def on_startup():
    # Пул подключений к БД создадим позже, когда появится DATABASE_URL
    if not DB_URL:
        print("⚠️ DATABASE_URL не задан — это ок для первого деплоя без БД.")
        app.state.pool = None
        return
    app.state.pool = await asyncpg.create_pool(DB_URL, max_size=10)
    # На старте создадим базовые таблицы (idempotent)
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT UNIQUE,
            site_url TEXT,
            tg_contact TEXT,
            city TEXT,
            phone TEXT,
            commission_pct NUMERIC(5,2) DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS cars (
            id BIGSERIAL PRIMARY KEY,
            partner_id INT REFERENCES partners(id),
            external_id TEXT,
            title TEXT,
            brand TEXT,
            model TEXT,
            year INT,
            class TEXT,
            transmission TEXT,
            fuel TEXT,
            seats INT,
            city TEXT,
            price_per_day NUMERIC(12,2),
            currency TEXT,
            deposit NUMERIC(12,2),
            deposit_currency TEXT,
            with_driver BOOLEAN,
            free_km_per_day INT,
            photos JSONB,
            conditions JSONB,
            source_url TEXT,
            available BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMPTZ DEFAULT now(),
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(partner_id, external_id)
        );
        CREATE TABLE IF NOT EXISTS leads (
            id BIGSERIAL PRIMARY KEY,
            user_name TEXT,
            phone TEXT,
            city TEXT,
            car_id BIGINT REFERENCES cars(id),
            partner_id INT REFERENCES partners(id),
            note TEXT,
            status TEXT DEFAULT 'new',
            utm JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS cars_partner_idx ON cars(partner_id);
        CREATE INDEX IF NOT EXISTS cars_city_idx ON cars(city);
        CREATE INDEX IF NOT EXISTS cars_price_idx ON cars(price_per_day);
        """)
    print("✅ Startup: tables are ensured.")

@app.on_event("shutdown")
async def on_shutdown():
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()

@app.get("/cars")
async def list_cars(
    city: Optional[str] = Query(None),
    car_class: Optional[str] = Query(None, alias="class"),
    max_price: Optional[int] = Query(None),
    with_driver: Optional[bool] = Query(None),
    limit: int = 20,
    offset: int = 0,
):
    if not getattr(app.state, "pool", None):
        # БД ещё не подключали — вернём пустой список, это ок на первом шаге
        return []
    sql = ["SELECT id, title, brand, model, year, class, city, price_per_day, currency, photos FROM cars WHERE available = TRUE"]
    args: List = []
    if city:
        sql.append(f"AND city ILIKE ${len(args)+1}"); args.append(f"%{city}%")
    if car_class:
        sql.append(f"AND class ILIKE ${len(args)+1}"); args.append(f"%{car_class}%")
    if max_price is not None:
        sql.append(f"AND price_per_day <= ${len(args)+1}"); args.append(max_price)
    if with_driver is not None:
        sql.append(f"AND with_driver = ${len(args)+1}"); args.append(with_driver)
    sql.append("ORDER BY price_per_day ASC NULLS LAST, updated_at DESC")
    sql.append(f"LIMIT ${len(args)+1} OFFSET ${len(args)+2}"); args += [limit, offset]
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(" ".join(sql), *args)
    return [dict(r) for r in rows]
