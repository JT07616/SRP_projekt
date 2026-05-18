# Imports
import pandas as pd
import json
import requests
import random
from pathlib import Path
from sqlalchemy import Date
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, insert
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import List, Dict, Any

# Putanja do predprocesirane CSV datoteke.
# Relacijski model se puni iz 80% ociscenih podataka.
CSV_FILE_PATH = Path(__file__).resolve().parent / "processed" / "atp_matches_processed_80.csv"

# Učitavanje CSV datoteke u dataframe
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")  # Print dataset size
print(df.head())  # Preview first few rows

# Database Connection
Base = declarative_base()

# Definiranje sheme baze podataka
# --------------------------------------------------------------
class Country(Base):
    __tablename__ = 'country'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    population = Column(Integer, nullable=False)
    region = Column(String(45), nullable=False)

class Tournament(Base):
    __tablename__ = 'tournament'

    id = Column(Integer, primary_key=True)  # tourney_id
    name = Column(String(100), nullable=False)
    surface = Column(String(20))
    draw_size = Column(Integer)
    level = Column(String(10))
    start_date = Column(Date)
    city = Column(String(50))
    country_fk = Column(Integer, ForeignKey('country.id'))


class Player(Base):
    __tablename__ = 'player'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    hand = Column(String(5))
    height = Column(Integer)
    country_fk = Column(Integer, ForeignKey('country.id'))

class Match(Base):
    __tablename__ = 'match'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_fk = Column(Integer, ForeignKey('tournament.id'))
    match_num = Column(Integer)
    score = Column(String(100))
    best_of = Column(Integer)
    round = Column(String(20))
    minutes = Column(Integer)
    winner_fk = Column(Integer, ForeignKey('player.id'))
    loser_fk = Column(Integer, ForeignKey('player.id'))


class PlayerMatchStats(Base):
    __tablename__ = 'player_match_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    match_fk = Column(Integer, ForeignKey('match.id'))
    player_fk = Column(Integer, ForeignKey('player.id'))
    is_winner = Column(Integer, nullable=False)   # oznacava pobjednika/gubitnika
    seed = Column(String(10))
    entry = Column(String(10))
    rank = Column(Integer)
    rank_points = Column(Integer)
    ace = Column(Integer)
    double_fault = Column(Integer)
    service_points = Column(Integer)
    first_in = Column(Integer)
    first_won = Column(Integer)
    second_won = Column(Integer)
    service_games = Column(Integer)
    break_points_saved = Column(Integer)
    break_points_faced = Column(Integer)



# Database Connection

print("="*50)
print("KORAK 1: Spajam se na bazu...")

engine = create_engine('mysql+pymysql://root:root@localhost:3306/tenis', echo=False)

print("KORAK 2: Brišem postojeće tablice...")
Base.metadata.drop_all(engine)

print("KORAK 3: Kreiram nove tablice...")
Base.metadata.create_all(engine)

print("KORAK 4: Tablice kreirane, provjera...")
from sqlalchemy import inspect
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"Tablice u bazi NAKON kreiranja: {tables}")
print("="*50)

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Otvori novu sesiju

# --------------------------------------------------------------
# Import podataka
# ------------------------------------------------------------

# **1. Umetanje zemalja**
countries = pd.concat([
    df[['tourney_country']].rename(columns={'tourney_country': 'name'}),
    df[['winner_country']].rename(columns={'winner_country': 'name'}),
    df[['loser_country']].rename(columns={'loser_country': 'name'})
]).drop_duplicates().dropna()

countries_list = []

for idx, (i, row) in enumerate(countries.iterrows(), 1):

    original_name = row['name']

    try:
        response = requests.get(f"https://restcountries.com/v3.1/name/{original_name}?fullText=true")
        data = json.loads(response.content)

        if isinstance(data, list) and len(data) > 0:
            country_entry = {
                "id": idx,
                "name": original_name,
                "population": data[0].get('population', 0),
                "region": data[0].get('region', 'Unknown')
            }
        else:
            raise Exception("Nema podataka")

    except:
        print(f"⚠ Problem s državom: {original_name}")

        country_entry = {
            "id": idx,
            "name": original_name,
            "population": 0,
            "region": "Unknown"
        }

    countries_list.append(country_entry)

session.execute(insert(Country), countries_list)  # Bulk insert
session.commit()

print("Countries inserted!")

country_map = {c.name: c.id for c in session.query(Country).all()}


# **2. Umetanje turnira**
tournaments = df[['tourney_id', 'tourney_name', 'surface', 'draw_size',
                  'tourney_level', 'tourney_date', 'tourney_city', 'tourney_country']].drop_duplicates().copy()

# Pretvori tourney_date u datum
tournaments['tourney_date'] = pd.to_datetime(tournaments['tourney_date']).dt.date

# Mapiranje države iz teksta u ID
tournaments['country_fk'] = tournaments['tourney_country'].map(country_map)

# Preimenovanje stupaca da odgovaraju tablici
tournaments = tournaments.rename(columns={
    'tourney_id': 'id',
    'tourney_name': 'name',
    'tourney_level': 'level',
    'tourney_date': 'start_date',
    'tourney_city': 'city'
})

# Izbacujemo originalni tekstualni stupac države jer sada imamo country_fk
tournaments = tournaments.drop(columns=['tourney_country'])

# Pretvori u listu rječnika sa string ključevima
tournaments_list = [{str(k): v for k, v in row.items()} for row in tournaments.to_dict(orient="records")]

# Bulk insert
session.execute(insert(Tournament), tournaments_list)
session.commit()

print("Tournaments inserted!")

tournament_map = {t.id: t.id for t in session.query(Tournament).all()}
# Vrijednost mapiranja: {tourney_id: tourney_id}


# **3. Umetanje igrača**

# WINNER dio
players_winner = df[['winner_id', 'winner_name', 'winner_hand', 'winner_ht', 'winner_country']].copy()

players_winner = players_winner.rename(columns={
    'winner_id': 'id',
    'winner_name': 'name',
    'winner_hand': 'hand',
    'winner_ht': 'height',
    'winner_country': 'country'
})

# LOSER dio
players_loser = df[['loser_id', 'loser_name', 'loser_hand', 'loser_ht', 'loser_country']].copy()

players_loser = players_loser.rename(columns={
    'loser_id': 'id',
    'loser_name': 'name',
    'loser_hand': 'hand',
    'loser_ht': 'height',
    'loser_country': 'country'
})

# SPOJI winner + loser
players = pd.concat([players_winner, players_loser])

# Makni duplikate (ključ = id)
players = players.drop_duplicates(subset=['id'])

# Mapiranje države
players['country_fk'] = players['country'].map(country_map)

# Makni tekstualni country
players = players.drop(columns=['country'])

# Pretvori u listu dict-ova
players_list = [{str(k): v for k, v in row.items()} for row in players.to_dict(orient="records")]

# Insert
session.execute(insert(Player), players_list)
session.commit()

print("Players inserted!")

# Map (trebat će kasnije)
player_map = {p.id: p.id for p in session.query(Player).all()}


# **4. Umetanje mečeva**
matches = df[['tourney_id', 'match_num', 'score', 'best_of', 'round', 'minutes',
              'winner_id', 'loser_id']].copy()

# Preimenovanje stupaca da odgovaraju tablici
matches = matches.rename(columns={
    'tourney_id': 'tournament_fk',
    'winner_id': 'winner_fk',
    'loser_id': 'loser_fk'
})

# Pretvori u listu rječnika sa string ključevima
matches_list = [{str(k): v for k, v in row.items()} for row in matches.to_dict(orient="records")]

# Bulk insert
session.execute(insert(Match), matches_list)
session.commit()

print("Matches inserted!")

# **5. Umetanje player_match_stats**

stats_list = []

for idx, row in df.iterrows():
    match_id = idx + 1  # jer match.id je autoincrement i ide redom

    # ------------------
    # WINNER
    # ------------------
    winner_stats = {
        "match_fk": match_id,
        "player_fk": row['winner_id'],
        "is_winner": 1,

        "seed": row['winner_seed'],
        "entry": row['winner_entry'],
        "rank": row['winner_rank'],
        "rank_points": row['winner_rank_points'],

        "ace": row['w_ace'],
        "double_fault": row['w_df'],
        "service_points": row['w_svpt'],
        "first_in": row['w_1stIn'],
        "first_won": row['w_1stWon'],
        "second_won": row['w_2ndWon'],
        "service_games": row['w_SvGms'],
        "break_points_saved": row['w_bpSaved'],
        "break_points_faced": row['w_bpFaced']
    }

    stats_list.append(winner_stats)

    # ------------------
    # LOSER
    # ------------------
    loser_stats = {
        "match_fk": match_id,
        "player_fk": row['loser_id'],
        "is_winner": 0,

        "seed": row['loser_seed'],
        "entry": row['loser_entry'],
        "rank": row['loser_rank'],
        "rank_points": row['loser_rank_points'],

        "ace": row['l_ace'],
        "double_fault": row['l_df'],
        "service_points": row['l_svpt'],
        "first_in": row['l_1stIn'],
        "first_won": row['l_1stWon'],
        "second_won": row['l_2ndWon'],
        "service_games": row['l_SvGms'],
        "break_points_saved": row['l_bpSaved'],
        "break_points_faced": row['l_bpFaced']
    }

    stats_list.append(loser_stats)

# Insert
session.execute(insert(PlayerMatchStats), stats_list)
session.commit()

print("PlayerMatchStats inserted!")

print("Data imported successfully!")
