# -*- coding: utf-8 -*-
"""
Input for query assumes:
First element: author, published, movie, book, director, released
Second element: books, movies
Third element: title of book or movie
Other keywords: load data, help, quit

Database: (table: field)
books: key, book, author, published, movie_key
movies: key, movie, director, released, book_key
"""
import csv
import os.path
import sqlite3

###############################################################################

# Database function to load data from CSVs into database file
def load_data():
    # Drops the books table if it already exists
    c.execute("DROP TABLE IF EXISTS books")
    # Creates table to be filled with books, sets up column names/data types
    c.execute("CREATE TABLE books (key INTEGER, book TEXT, author TEXT, " +
              "published INTEGER, movie_key INTEGER)")
    # Takes in books from CSV file
    book_csv = open("books.csv")
    book_reader = csv.reader(book_csv)
    next(book_reader) # Skip header in CSV file
    # Inserts books into books table
    c.executemany("INSERT INTO books VALUES (?, ?, ?, ?, ?)", book_reader)
    conn.commit() # Commits changes to database

    # Drops the movies table if it already exists
    c.execute("DROP TABLE IF EXISTS movies")
    # Creates table to be filled with movies, sets up column names/data types
    c.execute("CREATE TABLE movies (key INTEGER, movie TEXT, director TEXT, " +
              "released INTEGER, book_key INTEGER)")
    # Takes in movies from CSV file
    movie_csv = open("movies.csv")
    movie_reader = csv.reader(movie_csv)
    next(movie_reader) # Skip header in CSV file
    # Inserts movies into movies table
    c.executemany("INSERT INTO movies VALUES (?, ?, ?, ?, ?)", movie_reader)
    conn.commit() # Commits changes to database

# Database function to quit the database connection
def quit_db():
    if conn:
        conn.close() # Closes database connection

# Database function to execute query
def query_db(field, table, entry):
    # Entry is stripped of any leading or trailing quotation marks
    # And search is performed on a tuple, so entry is made into a tuple
    search = ((entry.strip('\'" ')),)
    # Title is name of column to be searched for entry: either 'book' or 'movie'
    title = table[:-1] # Get title string from given table name
    # Perform search of database; all searches are performed on joined database
    c.execute("SELECT " + field + " FROM books" +
              " JOIN movies ON books.movie_key = movies.key" +
              " WHERE " + title + "= ?", search)
    try: # Try to fetch result returned from database
        return [(c.fetchone()[0])]
    except: # If no result found, returns error
        return ["Error: entry not found in database"]

###############################################################################

# Function gets input from user for query and returns keys as a list
def parse_input():
    input_string = ""
    # Get user input for query
    while (input_string == ""): # While user enters nothing, keep asking for input
        input_string = input("Please enter your query, 'quit' or 'help': ")

    # If input is 'load data', 'help', or 'quit', return appropriate term as list
    if input_string.lower().strip('\'"') == "load data":
        return ["load"]
    elif input_string.lower().strip('\'"') == "help":
        return ["help"]
    elif input_string.lower().strip('\'"') == "quit":
        return ["quit"]
    else: # Otherwise, check if it's a valid query
        values = handling_error(input_string)
        if len(values) < 3: # Only errors found if fewer than 3 in returned list
            return values   # (maximum of 2 errors returned)
        else: # Otherwise, run query
            result = query_db(values[0], values[1], values[2])
            return result

###############################################################################

# Function to check if query is properly formatted
def handling_error(user_input):
    # Fields available in the database
    field_key = ["book", "movie", "author", "director", "published", "released"]
    # Tables available in the database
    table_key = ["books", "movies"]

    # Split user input into a list: 1st word should be field, 2nd a table
    user_list = user_input.split(maxsplit=2)
    return_list = [""] # Holds list of errors or query elements to be returned
    error_found = False # No errors have been found yet

    # Check if list of parsed input is 3 (i.e. field, table, search entry)
    if len(user_list) != 3:
        return ["Error: not a valid entry. Enter 'help' for help"]
    # Now, length must equal 3, so check elements to make sure the query is valid
    else :
        # Change first two elements to lower case, strip of leading/trailing quotes
        user_list[0] = user_list[0].lower().strip('\'" ')
        user_list[1] = user_list[1].lower().strip('\'" ')

        # Check to see if the first word in the query is a valid field
        if user_list[0] not in field_key:
            return_list[0] = "Error: first word is not an available field"
            error_found = True

        # Check to see if the second word in the query is a valid table
        if user_list[1] not in table_key:
            # If there is already an error, append message to end of return_list
            if error_found == True:
                return_list.append("Error: second word is not an available table")
            else: # If no current error, add error message to first index
                return_list[0] = "Error: second word is not an available table"
                error_found = True

        # Complie the return list of user inputs if no errors found
        if error_found == False:
            return_list[0] = user_list[0]
            return_list.append(user_list[1])
            return_list.append(user_list[2])

        return return_list # Return list of error(s) or user inputs

###############################################################################

# Function prints help message on workings of program, details of query language
def help_message():
    print(" The available actions in this program are:")
    print("\t Help \n\t Load Data \n\t Query Search \n\t Quit")
    print("\n Scroll to the section you would like information on.")

    print("\n Help:")
    print("\t This command will print this statement to assist the user in" +
          "\n\t navigating the program.")

    print("\n Load Data:")
    print("\t This command must be run the first time you use this program" +
          "\n\t and will read data from CSV files and input it into the database" +
          "\n\t to allow for querying.")

    print("\n Query Search:")
    print("\t A query search must contain a field, a table, and a search term.")
    print("\t The format of the search should be: 'field' 'table' 'search title'.")
    print("\t An example query: movie books Wiseguy" +
          "\n\t which would return the movie based on the book 'Wiseguy': Goodfellas")

    print("\n\t You can search for information about a book using the fields:" +
          "\n\t\t -> author" +
          "\n\t\t -> published (for the year the book was published)" +
          "\n\t\t -> movie (for the title of the movie the book was adapted into)" +
          "\n\t\t -> director (of the corresponding movie)" +
          "\n\t\t -> released (for the year the corresponding movie was released)")

    print("\n\t Similarly, you can search for information about a movie using the fields:" +
          "\n\t\t -> director" +
          "\n\t\t -> released (for the year the movie was released)" +
          "\n\t\t -> book (for the title of the book from which the movie was adapted)" +
          "\n\t\t -> author (of the corresponding book)" +
          "\n\t\t -> published (for the year the corresponding book was published)")

    print("\n\t The table must be 'books' or 'movies' and is chosen based on the" +
          "\n\t kind of title you are using to search, so when searching using" +
          "\n\t the title of a book, the table is books and when the title is a" +
          "\n\t movie, the table is movies.")

    print("\n\t Please be aware that the titles of movies and books are case" +
          "\n\t sensitive and must match the database entry.")

    print("\n Quit:")
    print("\t This command will close the connection with the database and" +
          "\n\t terminate the program.")

###############################################################################

print("Welcome to The Books & Their Movie Adaptations Database!")

# Checks if the database file exists in this directory
if os.path.isfile("books_movies.db"):
    # If file exists, opens connection and gets ready for queries
    conn = sqlite3.connect("books_movies.db")
    c = conn.cursor()
else:
    # If file does not exist, requests user 'load data' or ask for 'help'
    # User must load data to proceed with program
    input_string = ""
    while ((input_string.lower().strip('\'" ') != "load data")):
        if input_string.lower().strip('\'" ') == "help":
            help_message()
        input_string = input("The database must be loaded. Enter 'load data' or 'help': ")
    # Once user enters 'load data', make connection and load data
    print("Loading...")
    conn = sqlite3.connect("books_movies.db")
    c = conn.cursor()
    load_data()

print("Example Query: author books The Hobbit") # Quotes not needed for title

values = [""] # Holds query result, error messages, or other commands (quit, load, help)
# Loop runs so user can enter many queries and commands. Terminates on entering 'quit'
while (values[0] != "quit"):
    values = parse_input()
    if values[0] == "load": # At this point, data is loaded but will reload on request
        load_data()
        print("Reloading...")
    elif values[0] == "help": help_message()
    elif values[0] != "quit": # Will be query result or error messages
        for value in values:
            print(value)

print("Terminating program, have a nice day!")
quit_db()
