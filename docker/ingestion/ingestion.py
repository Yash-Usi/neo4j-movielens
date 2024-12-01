import csv
import os
import time
from pathlib import Path
from neo4j import GraphDatabase

# Wait for Neo4j to be ready in Docker
time.sleep(15)

# Paths
DATA_PATH = Path("data/")
MOVIE_PATH = DATA_PATH / "movies.csv"
RATINGS_PATH = DATA_PATH / "ratings.csv"
LINKS_PATH = DATA_PATH / "ratings.csv"
TAGS_PATH = DATA_PATH / "tags.csv"

# Neo4j connection settings
HOST = os.environ.get("NEO4J_HOST", "localhost")
PORT = 7687
USER = "neo4j"
PASS = "8769005670"  # default

# Number of rows to process (limits for testing purposes)
N_MOVIES = 1000
N_RATINGS = 1000
N_TAGS = 1000
N_LINKS = 1000


class Neo4jIngestion:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_genre_nodes(self):
        genres = ["Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                  "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
                  "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]

        with self.driver.session() as session:
            for genre in genres:
                session.run("MERGE (g:Genre {name: $name})", name=genre)

    def load_movies(self):
        with open(MOVIE_PATH) as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            for i, row in enumerate(reader):
                self.create_movie_node(row)
                self.create_genre_relationships(row)

                if i % 1000 == 0:
                    print(f"{i}/{N_MOVIES} Movie nodes created")
                if i >= N_MOVIES:
                    break

    def create_movie_node(self, row):
        movie_id = row[0]
        title = row[1][:-7]
        year = row[1][-5:-1]
        with self.driver.session() as session:
            session.run(
                "MERGE (m:Movie {id: $id, title: $title, year: $year})",
                id=movie_id, title=title, year=year
            )

    def create_genre_relationships(self, row):
        movie_id = row[0]
        genres = row[2].split("|")
        with self.driver.session() as session:
            for genre in genres:
                session.run(
                    """
                    MATCH (m:Movie {id: $movie_id}), (g:Genre {name: $genre})
                    MERGE (g)-[:IS_GENRE_OF]->(m)
                    """,
                    movie_id=movie_id, genre=genre
                )

    def load_ratings(self):
        with open(RATINGS_PATH) as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            for i, row in enumerate(reader):
                self.create_user_node(row[0])
                self.create_rating_relationship(row)

                if i % 100 == 0:
                    print(f"{i}/{N_RATINGS} Rating relationships created")
                if i >= N_RATINGS:
                    break

    def create_user_node(self, user_id):
        with self.driver.session() as session:
            session.run("MERGE (u:User {id: $id})", id=f"User {user_id}")

    def create_rating_relationship(self, row):
        user_id = f"User {row[0]}"
        movie_id = row[1]
        rating = float(row[2])
        timestamp = row[3]
        with self.driver.session() as session:
            session.run(
                """
                MATCH (u:User {id: $user_id}), (m:Movie {id: $movie_id})
                MERGE (u)-[:RATED {rating: $rating, timestamp: $timestamp}]->(m)
                """,
                user_id=user_id, movie_id=movie_id, rating=rating, timestamp=timestamp
            )

    def load_tags(self):
        with open(TAGS_PATH) as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            for i, row in enumerate(reader):
                self.create_tag_relationship(row)

                if i % 100 == 0:
                    print(f"{i}/{N_TAGS} Tag relationships created")
                if i >= N_TAGS:
                    break

    def create_tag_relationship(self, row):
        user_id = f"User {row[0]}"
        movie_id = row[1]
        tag = row[2]
        timestamp = row[3]
        with self.driver.session() as session:
            session.run(
                """
                MATCH (u:User {id: $user_id}), (m:Movie {id: $movie_id})
                MERGE (u)-[:TAGGED {tag: $tag, timestamp: $timestamp}]->(m)
                """,
                user_id=user_id, movie_id=movie_id, tag=tag, timestamp=timestamp
            )

    def load_links(self):
        with open(LINKS_PATH) as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            for i, row in enumerate(reader):
                self.update_movie_links(row)

                if i % 100 == 0:
                    print(f"{i}/{N_LINKS} Movie nodes updated with links")
                if i >= N_LINKS:
                    break

    def update_movie_links(self, row):
        movie_id = row[0]
        imdb_id = row[1]
        tmdb_id = row[2]
        with self.driver.session() as session:
            session.run(
                """
                MATCH (m:Movie {id: $movie_id})
                SET m.imdbId = $imdb_id, m.tmdbId = $tmdb_id
                """,
                movie_id=movie_id, imdb_id=imdb_id, tmdb_id=tmdb_id
            )


if __name__ == "__main__":
    uri = f"bolt://{HOST}:{PORT}"
    print(uri)
    ingestion = Neo4jIngestion(uri, USER, PASS)
    ingestion.create_genre_nodes()
    ingestion.load_movies()
    ingestion.load_ratings()
    ingestion.load_tags()
    ingestion.load_links()
    ingestion.close()
