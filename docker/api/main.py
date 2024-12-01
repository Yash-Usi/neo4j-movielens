from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from pydantic import BaseModel
import os

app = FastAPI()

# Neo4j connection settings
NEO4J_HOST = os.environ.get("NEO4J_HOST", "localhost")
PORT = 7687
USER = "neo4j"
PASS = "8769005670"

driver = GraphDatabase.driver(f"bolt://{NEO4J_HOST}:{PORT}", auth=(USER, PASS))

# Helper function to query Neo4j
def run_query(query, parameters=None):
    with driver.session() as session:
        result = session.run(query, parameters)
        return [record.data() for record in result]

####### Movie Endpoints #######

@app.get("/api/movie/details/{title}")
def get_movie_data(title: str):
    query = """
        MATCH (m:Movie {title: $title})
        RETURN m
    """
    result = run_query(query, {"title": title})
    if not result:
        raise HTTPException(status_code=404, detail="Movie not found")
    return result[0]

@app.get("/api/movie/genres/{title}")
def get_movie_genres(title: str):
    query = """
        MATCH (g:Genre)-[:IS_GENRE_OF]->(m:Movie {title: $title})
        RETURN g.name AS genre
    """
    result = run_query(query, {"title": title})
    return {"genres": [record["genre"] for record in result]}

@app.get("/api/movie/ratings/{title}")
def get_movie_ratings(title: str):
    query = """
        MATCH (u:User)-[r:RATED]->(m:Movie {title: $title})
        RETURN u.id AS user, r.rating AS rating
    """
    result = run_query(query, {"title": title})
    return result

@app.get("/api/movie/tags/{title}")
def get_movie_tags(title: str):
    query = """
        MATCH (u:User)-[t:TAGGED]->(m:Movie {title: $title})
        RETURN u.id AS user, t.tag AS tag
    """
    result = run_query(query, {"title": title})
    return result

@app.get("/api/movie/year/{year}")
def get_movies_by_year(year: int):
    query = """
        MATCH (m:Movie) WHERE m.year = $year
        RETURN m.title AS title, m.year AS year
    """
    result = run_query(query, {"year": year})
    return result

@app.get("/api/movie/average-rating/{title}")
def get_movie_average_rating(title: str):
    query = """
        MATCH (u:User)-[r:RATED]->(m:Movie {title: $title})
        RETURN avg(r.rating) AS averageRating
    """
    result = run_query(query, {"title": title})
    return {"averageRating": result[0]["averageRating"]} if result else {"averageRating": None}

####### Top Movies Endpoints #######

@app.get("/api/top/movie/top-n/{n}")
def get_movie_top_n(n: int):
    query = """
        MATCH (u:User)-[r:RATED]->(m:Movie)
        RETURN m.title AS title, avg(r.rating) AS averageRating
        ORDER BY averageRating DESC LIMIT $n
    """
    result = run_query(query, {"n": n})
    return result

@app.get("/api/top/movie/n-most-rated/{n}")
def get_movie_n_most_rated(n: int):
    query = """
        MATCH (u:User)-[r:RATED]->(m:Movie)
        RETURN m.title AS title, count(r.rating) AS numberOfRatings
        ORDER BY numberOfRatings DESC LIMIT $n
    """
    result = run_query(query, {"n": n})
    return result

####### User Endpoints #######

@app.get("/api/user/ratings/{user_id}")
def get_user_ratings(user_id: str):
    query = """
        MATCH (u:User {id: $user_id})-[r:RATED]->(m:Movie)
        RETURN m.title AS movie, r.rating AS rating
    """
    result = run_query(query, {"user_id": user_id})
    return result

@app.get("/api/user/tags/{user_id}")
def get_user_tags(user_id: str):
    query = """
        MATCH (u:User {id: $user_id})-[t:TAGGED]->(m:Movie)
        RETURN m.title AS title, t.tag AS tag
    """
    result = run_query(query, {"user_id": user_id})
    return result

@app.get("/api/user/average-rating/{user_id}")
def get_user_average_rating(user_id: str):
    query = """
        MATCH (u:User {id: $user_id})-[r:RATED]->(m:Movie)
        RETURN avg(r.rating) AS averageRating
    """
    result = run_query(query, {"user_id": user_id})
    return {"averageRating": result[0]["averageRating"]} if result else {"averageRating": None}

####### Recommender Engine #######

@app.get("/api/rec_engine/content/{title}/{n}")
def get_recommendation_content(title: str, n: int):
    query = """
        MATCH (m:Movie {title: $title})<-[:IS_GENRE_OF]-(g:Genre)-[:IS_GENRE_OF]->(rec:Movie)
        WITH rec, COLLECT(g.name) AS genres, COUNT(*) AS numberOfSharedGenres
        RETURN rec.title AS title, genres, numberOfSharedGenres
        ORDER BY numberOfSharedGenres DESC LIMIT $n
    """
    result = run_query(query, {"title": title, "n": n})
    return result

@app.get("/api/rec_engine/collab/{user_id}/{n}")
def get_recommendation_collaborative(user_id: str, n: int):
    query = """
        MATCH (u1:User {id: $user_id})-[r:RATED]->(m:Movie)
        WITH u1, avg(r.rating) AS u1_mean
        MATCH (u1)-[r1:RATED]->(m:Movie)<-[r2:RATED]-(u2)
        WITH u1, u1_mean, u2, COLLECT({r1: r1, r2: r2}) AS ratings WHERE size(ratings) > 10
        MATCH (u2)-[r:RATED]->(m:Movie)
        WITH u1, u1_mean, u2, avg(r.rating) AS u2_mean, ratings
        UNWIND ratings AS r
        WITH sum((r.r1.rating - u1_mean) * (r.r2.rating - u2_mean)) AS nom,
        sqrt(sum((r.r1.rating - u1_mean)^2) * sum((r.r2.rating - u2_mean)^2)) AS denom, u1, u2 WHERE denom <> 0
        WITH u1, u2, nom / denom AS pearson
        ORDER BY pearson DESC LIMIT 10
        MATCH (u2)-[r:RATED]->(m:Movie) WHERE NOT EXISTS((u1)-[:RATED]->(m))
        RETURN m.title AS title, SUM(pearson * r.rating) AS score
        ORDER BY score DESC LIMIT $n
    """
    result = run_query(query, {"user_id": user_id, "n": n})
    return result

@app.get("/api/database/all")
def get_all_database():
    query = """
        MATCH (n)
        RETURN n
    """
    result = run_query(query)
    return [record["n"] for record in result]