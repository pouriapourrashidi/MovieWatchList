import datetime
import sqlite3

CREATE_MOVIE_TABLE = "CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY, title TEXT,release_timestamp REAL);"

CREATE_USER_TABLE = "CREATE TABLE IF NOT EXISTS user(username TEXT PRIMARY KEY);"

CREATE_WATCH_LIST_TABLE = """ CREATE TABLE IF NOT EXISTS watched (
    user_username TEXT,
    movie_id INTEGER,
    FOREIGN KEY(user_username) REFERENCES user(username),
    FOREIGN KEY(movie_id) REFERENCES movies(id));
"""

INSERT_MOVIES = "INSERT INTO movies (title,release_timestamp) VALUES (?,?);"
SELECT_ALL_MOVIES = "SELECT * FROM movies;"
SELECT_UPCOMING_MOVIES = "SELECT * FROM movies WHERE release_timestamp > ?;"
INSERT_USER = "INSERT INTO user(username) VALUES (?);"
SELECT_WATCHED_MOVIES = "SELECT movies.* FROM watched " \
                        "JOIN movies ON movies.id = watched.movie_id " \
                        "JOIN user ON watched.user_username=user.username WHERE user.username = ?;"
INSERT_MOVIES_WATCHED = "INSERT INTO watched VALUES (?,?);"
DELETE_MOVIE = "DELETE FROM movies WHERE title = ?;"
SEARCH_MOVIE = "SELECT * FROM movies WHERE title LIKE ?;"
RELEASE_INDEX = "CREATE INDEX IF NOT EXISTS inx_release_timestamp ON movies(release_timestamp);"


connection = sqlite3.Connection("data.db")


def create_table():
    with connection:
        connection.execute(CREATE_MOVIE_TABLE)
        connection.execute(CREATE_USER_TABLE)
        connection.execute(CREATE_WATCH_LIST_TABLE)
        connection.execute(RELEASE_INDEX)

def add_user(username):
    with connection:
        connection.execute(INSERT_USER,(username,))


def add_movie(title, release_timestamp):
    with connection:
        connection.execute(INSERT_MOVIES, (title, release_timestamp))


def get_movies(upcoming=False):
    with connection:
        cursor = connection.cursor()
        if upcoming:
            right_now = datetime.datetime.today().timestamp()
            cursor.execute(SELECT_UPCOMING_MOVIES, (right_now,))
        else:
            cursor.execute(SELECT_ALL_MOVIES)

        return cursor.fetchall()

def search_movie(search_term):
    with connection:
        cursor = connection.cursor()
        cursor.execute(SEARCH_MOVIE,(f"%{search_term}%",))
        return cursor.fetchall()



def watch_movie(watcher, movie_id):
    with connection:
        connection.execute(INSERT_MOVIES_WATCHED, (watcher, movie_id))


def get_watched_movies(watcher):
    with connection:
        cursor = connection.cursor()
        cursor.execute(SELECT_WATCHED_MOVIES, (watcher,))
        return cursor.fetchall()
