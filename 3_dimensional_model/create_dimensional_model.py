'''
Skripta za generiranje dimenzijskog modela podataka -> star schema
Ova skripta demonstrira cijeli proces:
1. Konekcija na bazu podataka
2. Definicija modela (dimenzije i činjenice)
3. Brisanje starih tablica
4. Kreiranje novih tablica
5. ETL proces - punjenje dimenzijskih i fact tablica
6. Verifikacija rezultata
'''

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text

# Konekcija na bazu podataka
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/tenis"

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


# ========== DEFINICIJA DIMENZIJSKIH I FACT TABLICA ==========
# U dimenzijskom modelu (star schema):
# - Dimenzijske tablice (dim_) sadrže opisne atribute - TKO, GDJE, KADA, KONTEKST
# - Fact tablica (fact_) sadrži mjerljive numeričke vrijednosti - statistike igrača u meču
# - Surogatni ključevi (*_tk) koriste se za povezivanje fact tablice s dimenzijama

class DimPlayer(Base):
    __tablename__ = 'dim_player'

    # Surogatni ključ dimenzije igrača
    player_tk = Column(BigInteger, primary_key=True, autoincrement=True)

    # Poslovni atributi dimenzije
    player_id = Column(Integer, index=True)
    name = Column(String(100))
    hand = Column(String(5))
    height = Column(Integer)
    country_name = Column(String(45))
    region = Column(String(45))

# Hijerarhije:
# 1. Lokacijska: region -> country_name -> city -> name
# 2. Turnirska: level -> name
class DimTournament(Base):
    __tablename__ = 'dim_tournament'

    # Surogatni ključ dimenzije turnira
    tournament_tk = Column(BigInteger, primary_key=True, autoincrement=True)

    # Poslovni atributi dimenzije
    tournament_id = Column(Integer, index=True)
    name = Column(String(100))
    surface = Column(String(20))
    draw_size = Column(Integer)
    level = Column(String(10))
    city = Column(String(50))
    country_name = Column(String(45))
    region = Column(String(45))


class DimCountry(Base):
    __tablename__ = 'dim_country'

    # Surogatni ključ dimenzije države
    country_tk = Column(BigInteger, primary_key=True, autoincrement=True)

    # Poslovni atributi dimenzije
    country_id = Column(Integer, index=True)
    name = Column(String(45))
    population = Column(Integer)
    region = Column(String(45))

# Hijerarhija: year -> quarter -> month -> day
class DimDate(Base):
    __tablename__ = 'dim_date'

    # Surogatni ključ vremenske dimenzije
    date_tk = Column(Integer, primary_key=True, autoincrement=True)

    # Hijerarhija datuma omogućuje analizu po danu, mjesecu, kvartalu i godini
    date = Column(Date, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)

# Hijerarhija: round -> match
# round predstavlja fazu turnira (R32, R16, QF, SF, F)
class DimMatchInfo(Base):
    __tablename__ = 'dim_match_info'

    # Surogatni ključ dimenzije informacija o meču
    match_info_tk = Column(BigInteger, primary_key=True, autoincrement=True)

    # Opisni atributi konteksta meča i statusa igrača u meču
    score = Column(String(100))
    best_of = Column(Integer)
    round = Column(String(20))
    seed = Column(String(10))
    entry = Column(String(10))
    rank = Column(Integer)


class FactPlayerMatch(Base):
    __tablename__ = 'fact_player_match'

    # Primarni/surogatni ključ fact tablice
    fact_player_match_tk = Column(BigInteger, primary_key=True, autoincrement=True)

    # Strani ključevi prema svim dimenzijama (star schema)
    date_tk = Column(Integer, ForeignKey('dim_date.date_tk'))
    player_tk = Column(BigInteger, ForeignKey('dim_player.player_tk'))
    opponent_player_tk = Column(BigInteger, ForeignKey('dim_player.player_tk'))
    tournament_tk = Column(BigInteger, ForeignKey('dim_tournament.tournament_tk'))
    country_tk = Column(BigInteger, ForeignKey('dim_country.country_tk'))
    match_info_tk = Column(BigInteger, ForeignKey('dim_match_info.match_info_tk'))

    match_num = Column(Integer) # degenerirana dimenzija
     
    # Mjere - numeričke vrijednosti koje analiziramo
    ace = Column(Integer)
    double_fault = Column(Integer)
    service_points = Column(Integer)
    first_in = Column(Integer)
    first_won = Column(Integer)
    second_won = Column(Integer)
    service_games = Column(Integer)
    break_points_saved = Column(Integer)
    break_points_faced = Column(Integer)
    rank_points = Column(Integer)
    minutes = Column(Integer)


# ========== TESTIRANJE KONEKCIJE ==========

print("Testiram konekciju...")
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
    print("Konekcija uspješna!")


# ========== BRISANJE STARIH DIMENZIJSKIH TABLICA ==========
# Tablice se brišu u obrnutom redoslijedu od kreiranja zbog stranih ključeva:
# prvo fact tablica koja referencira dimenzije, zatim dimenzijske tablice.
# Ovo je korisno tijekom razvoja jer omogućuje ponovno pokretanje skripte.

print("\nBrišem postojeće dimenzijske tablice...")

try:
    FactPlayerMatch.__table__.drop(engine, checkfirst=True)
    DimMatchInfo.__table__.drop(engine, checkfirst=True)
    DimDate.__table__.drop(engine, checkfirst=True)
    DimCountry.__table__.drop(engine, checkfirst=True)
    DimTournament.__table__.drop(engine, checkfirst=True)
    DimPlayer.__table__.drop(engine, checkfirst=True)
    print("Postojeće dimenzijske tablice obrisane.")
except:
    print("Nema postojećih dimenzijskih tablica.")


# ========== KREIRANJE TABLICA ==========

print("\nKreiram dimenzijske i fact tablice...")
Base.metadata.create_all(engine)
print("Tablice kreirane!")


# ========== ETL PROCES ==========
# ETL = Extract (izvlačenje), Transform (transformacija), Load (punjenje)
# U checkpointu 3 izvor su relacijske tablice iz baze tenis, a cilj je star schema.

print("\nPopunjavam dimenzijske tablice...")


# 1. dim_player
# EXTRACT: dohvat igrača i države iz relacijskog modela
# TRANSFORM: spajanje igrača s državom i regijom
# LOAD: spremanje u dim_player
print("Popunjavam dim_player...")

players_query = """
SELECT DISTINCT
    p.id AS player_id,
    p.name AS name,
    p.hand AS hand,
    p.height AS height,
    c.name AS country_name,
    c.region AS region
FROM player p
LEFT JOIN country c ON p.country_fk = c.id;
"""

df_players = pd.read_sql(players_query, engine)


df_players.to_sql('dim_player', engine, if_exists='append', index=False)
print(f"Uneseno {len(df_players)} redaka u dim_player")


# 2. dim_tournament
# EXTRACT: dohvat turnira i države iz relacijskog modela
# TRANSFORM: spajanje turnira s lokacijskim atributima
# LOAD: spremanje u dim_tournament
print("Popunjavam dim_tournament...")

tournaments_query = """
SELECT DISTINCT
    t.id AS tournament_id,
    t.name AS name,
    t.surface AS surface,
    t.draw_size AS draw_size,
    t.level AS level,
    t.city AS city,
    c.name AS country_name,
    c.region AS region
FROM tournament t
LEFT JOIN country c ON t.country_fk = c.id;
"""

df_tournaments = pd.read_sql(tournaments_query, engine)
df_tournaments.to_sql('dim_tournament', engine, if_exists='append', index=False)
print(f"Uneseno {len(df_tournaments)} redaka u dim_tournament")


# 3. dim_country
# EXTRACT: dohvat država iz relacijskog modela
# TRANSFORM: zadržavanje poslovnih atributa države
# LOAD: spremanje u dim_country
print("Popunjavam dim_country...")

countries_query = """
SELECT DISTINCT
    c.id AS country_id,
    c.name AS name,
    c.population AS population,
    c.region AS region
FROM country c;
"""

df_countries = pd.read_sql(countries_query, engine)
df_countries.to_sql('dim_country', engine, if_exists='append', index=False)
print(f"Uneseno {len(df_countries)} redaka u dim_country")


# 4. dim_date
# EXTRACT: dohvat jedinstvenih datuma turnira
# TRANSFORM: rastavljanje datuma na dan, mjesec, kvartal i godinu
# LOAD: spremanje u dim_date
print("Popunjavam dim_date...")

dates_query = """
SELECT DISTINCT
    start_date
FROM tournament
WHERE start_date IS NOT NULL
ORDER BY start_date;
"""

df_dates = pd.read_sql(dates_query, engine)
df_dates['date'] = pd.to_datetime(df_dates['start_date'])

df_dates_final = pd.DataFrame({
    'date': df_dates['date'].dt.date,
    'day': df_dates['date'].dt.day,
    'month': df_dates['date'].dt.month,
    'quarter': df_dates['date'].dt.quarter,
    'year': df_dates['date'].dt.year
})

df_dates_final.to_sql('dim_date', engine, if_exists='append', index=False)
print(f"Uneseno {len(df_dates_final)} redaka u dim_date")


# 5. dim_match_info
# EXTRACT: dohvat rezultata, runde i statusa igrača u meču
# TRANSFORM: jedinstvene kombinacije opisa meča
# LOAD: spremanje u dim_match_info
print("Popunjavam dim_match_info...")

match_info_query = """
SELECT DISTINCT
    m.score AS score,
    m.best_of AS best_of,
    m.round AS round,
    pms.seed AS seed,
    pms.entry AS entry,
    pms.`rank` AS `rank`
FROM player_match_stats pms
INNER JOIN `match` m ON pms.match_fk = m.id;
"""

df_match_info = pd.read_sql(match_info_query, engine)
df_match_info.to_sql('dim_match_info', engine, if_exists='append', index=False)
print(f"Uneseno {len(df_match_info)} redaka u dim_match_info")


# ========== PUNJENJE FACT TABLICE ==========
# Fact tablica povezuje sve dimenzije i sadrži mjere/statistike igrača u meču.
# Jedan meč daje dva zapisa u player_match_stats: pobjednik i poraženi igrač.

print("\nPopunjavam fact_player_match...")

fact_query = """
INSERT INTO fact_player_match (
    date_tk,
    player_tk,
    opponent_player_tk,
    tournament_tk,
    country_tk,
    match_info_tk,
    match_num,
    ace,
    double_fault,
    service_points,
    first_in,
    first_won,
    second_won,
    service_games,
    break_points_saved,
    break_points_faced,
    rank_points,
    minutes
)
SELECT
    dd.date_tk, -- surogat datuma
    dp.player_tk, -- surogat igrača
    dop.player_tk AS opponent_player_tk,
    dt.tournament_tk, -- surogat turnira
    dc.country_tk, -- surogat države
    dmi.match_info_tk, -- surogat opisa meča
    m.match_num, -- degenerirana dimenzija
    pms.ace, -- mjere/statistike igrača
    pms.double_fault,
    pms.service_points,
    pms.first_in,
    pms.first_won,
    pms.second_won,
    pms.service_games,
    pms.break_points_saved,
    pms.break_points_faced,
    pms.rank_points,
    m.minutes
FROM player_match_stats pms
-- Svaki JOIN povezuje izvorne relacijske tablice s dimenzijama preko prirodnih ključeva
INNER JOIN `match` m ON pms.match_fk = m.id
INNER JOIN tournament t ON m.tournament_fk = t.id
INNER JOIN player p ON pms.player_fk = p.id
LEFT JOIN country c ON p.country_fk = c.id

INNER JOIN dim_player dp
    ON p.id = dp.player_id

INNER JOIN dim_player dop
    ON (
        CASE
            WHEN pms.player_fk = m.winner_fk THEN m.loser_fk
            ELSE m.winner_fk
        END
    ) = dop.player_id

INNER JOIN dim_tournament dt
    ON t.id = dt.tournament_id

INNER JOIN dim_country dc
    ON c.id = dc.country_id

INNER JOIN dim_date dd
    ON t.start_date = dd.date

INNER JOIN dim_match_info dmi
    ON IFNULL(m.score, '') = IFNULL(dmi.score, '')
    AND IFNULL(m.best_of, -1) = IFNULL(dmi.best_of, -1)
    AND IFNULL(m.round, '') = IFNULL(dmi.round, '')
    AND IFNULL(pms.seed, '') = IFNULL(dmi.seed, '')
    AND IFNULL(pms.entry, '') = IFNULL(dmi.entry, '')
    AND IFNULL(pms.`rank`, -1) = IFNULL(dmi.`rank`, -1);
"""

with engine.connect() as conn:
    result = conn.execute(text(fact_query))
    conn.commit()
    print(f"Uneseno {result.rowcount} redaka u fact_player_match")


# ========== VERIFIKACIJA ==========

print("\nBroj redaka po tablicama:")

tables = [
    'dim_player',
    'dim_tournament',
    'dim_country',
    'dim_date',
    'dim_match_info',
    'fact_player_match'
]

with engine.connect() as conn:
    for table in tables:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        print(f"{table}: {count} redaka")


print("\nUzorak podataka iz fact_player_match:")

preview_query = """
SELECT
    p.name AS player,
    op.name AS opponent,
    t.name AS tournament,
    c.name AS country,
    d.year,
    d.month,
    mi.round,
    mi.best_of,
    f.match_num,
    f.ace,
    f.double_fault,
    f.rank_points,
    f.minutes
FROM fact_player_match f
INNER JOIN dim_player p ON f.player_tk = p.player_tk
INNER JOIN dim_player op ON f.opponent_player_tk = op.player_tk
INNER JOIN dim_tournament t ON f.tournament_tk = t.tournament_tk
INNER JOIN dim_country c ON f.country_tk = c.country_tk
INNER JOIN dim_date d ON f.date_tk = d.date_tk
INNER JOIN dim_match_info mi ON f.match_info_tk = mi.match_info_tk
LIMIT 5;
"""

df_preview = pd.read_sql(preview_query, engine)
print(df_preview.to_string(index=False))

print("\nDIMENZIJSKI MODEL USPJEŠNO KREIRAN I POPUNJEN!")
