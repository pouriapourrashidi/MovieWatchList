import datetime
import database

menu = """Please select one of the following options:
1) Add new movie.
2) View upcoming movies.
3) View all movies
4) Watch a movie
5) View watched movies.
6) Add user
7) Search for a movie
8) Exit.

Your selection: """
welcome = "Welcome to the watchlist app!"

print(welcome)
database.create_table()


def promt_new_movie():
    title = input("Please enter the title of the movie: ")
    release_time = input("Please give the release time dd-mm-yyyy: ")
    timestamp = datetime.datetime.strptime(release_time, "%d-%m-%Y").timestamp()
    database.add_movie(title, timestamp)


def print_movies(heading, movies):
    print(f"--- {heading} movies : ---")
    for movie in movies:
        human_date = datetime.datetime.fromtimestamp(movie[2]).strftime("%b %d %Y")
        print(f"{movie[1]}--{human_date}")
    print("------------------------\n")


def promt_watched_movie():
    username = input("Username: ")
    movie_id = input("Please enter ID: ")
    database.watch_movie(username, movie_id)


def print_watched_movie(username, movies):
    print(f"{username} watched these movies: ")
    for movie in movies:
        print(f"{movie[1]}")
    print("------------------------------\n")


def promt_add_user():
    username = input("Username: ")
    database.add_user(username)

def promt_search_movie():
    term = input("Enter name of a movie: ")
    movies = database.search_movie(term)
    print_movies("List of Movies",movies)


while (user_input := input(menu)) != "8":
    if user_input == "1":
        promt_new_movie()
    elif user_input == "2":
        movies = database.get_movies(True)
        print_movies("upcoming", movies)
    elif user_input == "3":
        movies = database.get_movies(False)
        print_movies("all", movies)
    elif user_input == "4":
        promt_watched_movie()
    elif user_input == "5":
        username = input("Username: ")
        movies = database.get_watched_movies(username)
        print_watched_movie(username, movies)
    elif user_input == "6":
        promt_add_user()
    elif user_input=="7":
        promt_search_movie()
    else:
        print("Invalid input, please try again!")
