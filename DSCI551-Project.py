#!/usr/bin/env python
# coding: utf-8

# # Project -DSCI551

# ## Make my own database about movies 

# In[1]:


import sqlite3
import pandas as pd

# Connect to or create a SQLite database
conn = sqlite3.connect('movie_database.db')

# Create a cursor
cursor = conn.cursor()

# read two csv files 

films_df = pd.read_csv('films.csv')
directors_df = pd.read_csv('Oscar_Winners_ Director.csv')


# Create a table for films and directors
films_df.to_sql('films', conn, if_exists='replace', index=False)
directors_df.to_sql('Oscar_directors', conn, if_exists='replace', index=False)

# Commit changes and close connection
conn.commit()
conn.close()


# In[2]:


directors_df[directors_df['Nomination/Winner'] == 'Winner']


# In[3]:


films_df[films_df['Year'] == 2000]


# In[4]:


# delete data in the database

def delete_data(table, condition=None):
    conn = sqlite3.connect('movie_database.db')
    cursor = conn.cursor()

    # write the sql delete query
    if condition:
        query = f"DELETE FROM {table_name} WHERE {condition}"
    else:
        query = f"DELETE FROM {table_name}"

    try:
        cursor.execute(query)
        conn.commit()
        print(f"Data have successfully deleted from {table_name}")
    except sqlite3.Error as e:
        print("Error for deleting:", e)
    finally:
        conn.close()


# In[5]:


# delete_data('films', "year < 2010")


# In[6]:


# insert data in the database

def insert_data(table, data_df):
    conn = sqlite3.connect('movie_database.db')

    try:
        data_df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data have successfully inserted into {table_name}")
    except sqlite3.Error as e:
        print("Error inserting data:", e)
    finally:
        conn.close()


# In[7]:


# making the query language 

def query_movies_by_year(year, films_data):
    return films_data[films_data['Year'] == year]

def query_movies_by_title(title, films_data):
    return films_data[films_data['Title'] == title]

# def query_movies_by_director(director_name, films_data, directors_data):
#     director_name = directors_data[directors_data['Director(s)'] == director_name]
#     return films_data[films_data['Director'] == director_name]

def query_movies_by_director(director_name, films_data):
     return films_data[films_data['Director'] == director_name]

def query_directors_by_winner(director_data):
    return director_data[director_data['Nomination/Winner'] == 'Winner']


# In[12]:


user_query = input("Enter your query (format: 'movies by year 2010', 'movies by director Bobcat Goldthwait'): ")

if user_query.lower().startswith('movies by year'):
    year = int(user_query.split()[-1])
    result = query_movies_by_year(year, films_df)
    display(result)

elif user_query.lower().startswith('movies by title'):
    title = user_query.split('movies by title ')[-1]
    result = query_movies_by_title(title, films_df)
    display(result)
    
elif user_query.lower().startswith('movies by director'):
    director_name = user_query.split('movies by director ')[-1]
    result = query_movies_by_director(director_name, films_df)
    display(result)
    
elif user_query.lower().startswith('movies by Winner'):
    result = query_directors_by_winner(directors_df)
    display(result)

else:
    print("Invalid query")


# In[6]:


# combine two csv file to get the same director 
def query_movies_director_join(films_data, directors_data):
    merged_data = pd.merge(films_data, directors_data, left_on='Director', right_on='Director(s)', how='inner')
    return merged_data


# In[7]:


user_query = input("Enter your query (format: 'movies with directors'): ")

if user_query.lower().startswith('movies with directors'):
    result = query_movies_director_join(films_df, directors_df)
    print(result)
else:
    print("Invalid query")


# # MapReduce

# In[15]:


def map_movies(row):
    # For each movie record, emit director as key and movie title as value
    return (row['Director'], row['Title'])

movies_mapped = films_df.apply(map_movies, axis=1).tolist()


# In[16]:


from collections import defaultdict

def reduce_movies(movies_mapped):
    director_movies = defaultdict(list)

    # Reduce step to group movies by director
    for director, title in movies_mapped:
        director_movies[director].append(title)

    return director_movies


# In[17]:


result = reduce_movies(movies_mapped)

# Output the result
for director_id, movies in result.items():
    print(f"Director ID: {director_id}, Movies: {movies}")


# In[ ]:




