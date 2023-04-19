#!/usr/bin/env python3
import csv
import os

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

class SpotifyApp(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._create_constraints()


    def close(self):
        self.driver.close()


    def _create_constraints(self):
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT unique_user IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_artist IF NOT EXISTS FOR (a:Artist) REQUIRE a.artist_name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_song IF NOT EXISTS FOR (s:Song) REQUIRE (s.song_name, s.artist_name, s.album_name) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_album IF NOT EXISTS FOR (al:Album) REQUIRE (al.album_name, al.artist_name) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_playlist IF NOT EXISTS FOR (p:Playlist) REQUIRE (p.playlist_name, p.username) IS UNIQUE")


    def _create_user_node(self, username, followers, following, public_playlists):
        with self.driver.session() as session:
            try:
                session.run("CREATE (u:User {username: $username, followers: $followers, following: $following, public_playlists: $public_playlists})", 
                            username=username, followers=followers, following=following, public_playlists=public_playlists)
            except ConstraintError:
                pass


    def _create_artist_node(self, artist_name, monthly_listeners):
        with self.driver.session() as session:
            try:
                session.run("CREATE (a:Artist {artist_name: $artist_name, monthly_listeners: $monthly_listeners})", 
                            artist_name=artist_name, monthly_listeners=monthly_listeners)
            except ConstraintError:
                pass


    def _create_song_node(self, song_name, duration, artist_name, album_name):
        with self.driver.session() as session:
            try:
                session.run("CREATE (s:Song {song_name: $song_name, duration: $duration, artist_name: $artist_name, album_name: $album_name})", 
                            song_name=song_name, duration=duration, artist_name=artist_name, album_name=album_name)
            except ConstraintError:
                pass


    def _create_album_node(self, album_name, duration, release_date, artist_name):
        with self.driver.session() as session:
            try:
                session.run("CREATE (al:Album {album_name: $album_name, duration: $duration, release_date: $release_date, artist_name: $artist_name})", 
                            album_name=album_name, duration=duration, release_date=release_date, artist_name=artist_name)
            except ConstraintError:
                pass


    def _create_playlist_node(self, playlist_name, num_of_songs, username):
        with self.driver.session() as session:
            try:
                session.run("CREATE (p:Playlist {playlist_name: $playlist_name, num_of_songs: $num_of_songs, username: $username})", 
                            playlist_name=playlist_name, num_of_songs=num_of_songs, username=username)
            except ConstraintError:
                pass


    def _create_user_to_song_relationship(self, username, song):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User {username: $username}), (s:Song {song_name: $song_name})
                MERGE (u)-[:LIKES]->(s)
                RETURN u, s""", username=username, song_name=song)


    def _create_user_to_user_relationship(self, username1, username2):
        if username1 == username2:
            return #Avoid creating a relationship with itself
        with self.driver.session() as session:
            session.run("""
                MATCH (u1:User {username: $username1}), (u2:User {username: $username2})
                MERGE (u1)-[:FOLLOWS_USER]->(u2)
                RETURN u1, u2""", username1=username1, username2=username2)
            

    def _create_user_to_artist_relationship(self, username, artist):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User {username: $username}), (a:Artist {artist_name: $artist_name})
                MERGE (u)-[:FOLLOWS_ARTIST]->(a)
                RETURN u, a""", username=username, artist_name=artist)


    def _create_song_to_album_relationship(self, album):
        with self.driver.session() as session:
            session.run("""
                MATCH (s:Song), (al:Album)
                WHERE s.album_name=$album_name AND al.album_name=$album_name
                MERGE (s)-[r:FROM_ALBUM]->(al)
                RETURN type(r)""", album_name=album)
            

    def _create_album_to_artist_relationship(self, artist):
        with self.driver.session() as session:
            session.run("""
                MATCH (al:Album), (a:Artist)
                WHERE al.artist_name=$artist_name AND a.artist_name=$artist_name
                MERGE (al)-[r:FROM_ARTIST]->(a)
                RETURN type(r)""", artist_name=artist)      


    def _create_user_to_playlist_relationship(self, username):

        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (p:Playlist)
                WHERE u.username=$username AND p.username=$username
                MERGE (u)-[:CREATED]->(p)
                RETURN u, p""", username=username)
            
    def _create_song_to_playlist_relationship(self, song, playlist):
        with self.driver.session() as session:
            session.run("""
                MATCH (s:Song), (p:Playlist)
                MERGE (s)-[:IN_PLAYLIST]->(p)
                RETURN s, p""", song_name=song, playlist_name=playlist)
            

    def init(self, source):
        
        s_songname = None
        p_user = None
        al_artist = None
        p_playlist = None
        al_album = None

        for i in range(0, len(source)):
            with open(source[i], newline='') as csv_file:
                reader = csv.DictReader(csv_file,  delimiter='|')
                for r in reader:
                    if i == 0:
                        self._create_user_node(r["username"], r["followers"], r["following"], r["public_playlists"])
                    elif i == 1:
                        self._create_artist_node(r["artist_name"], r["monthly_listeners"])               
                    elif i == 2:
                        self._create_song_node(r["song_name"], r["duration"], r["artist_name"], r["album_name"])
                        s_songname = r["song_name"]
                    elif i == 3:
                        self._create_album_node(r["album_name"], r["duration"], r["release_date"], r["artist_name"])
                        al_album = r["album_name"]
                        al_artist = r["artist_name"]
                    elif i == 4:
                        self._create_playlist_node(r["playlist_name"], r["num_of_songs"], r["username"])
                        p_playlist = r["playlist_name"]
                        p_user = r["username"]
                    elif i == 5:
                        u_user1 = r["user"]
                        u_user2 = r["follows"]
                        self._create_user_to_user_relationship(u_user1, u_user2)
                    elif i == 6:
                        user = r["user"]
                        artist = r["artist"]
                        self._create_user_to_artist_relationship(user, artist)
                    else:
                        s_user = r["user"]
                        s_song = r["song"]
                        self._create_user_to_song_relationship(s_user, s_song)

                    self._create_user_to_playlist_relationship(p_user)
                    
                    if s_songname is not None or p_playlist is not None:                     
                        self._create_song_to_playlist_relationship(s_songname, p_playlist)
                                                    
                    if al_artist is not None:                                                                                                                                                    
                        self._create_album_to_artist_relationship(al_artist)                                                                                                                            
                                            
                    if al_album is not None:                        
                        self._create_song_to_album_relationship(al_album)
                        
                        

if __name__ == "__main__":
    file_list = ["data/users.csv", "data/artists.csv", "data/songs.csv", "data/albums.csv", "data/playlists.csv", "data/user_to_user.csv", "data/user_follows_artist.csv", "data/user_likes_song.csv"]
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'nevada-neptune-mambo-oasis-factor-4328')

    spotify = SpotifyApp(neo4j_uri, neo4j_user, neo4j_password)
    spotify.init(file_list)

    spotify.close()

