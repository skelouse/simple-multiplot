import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import sqlite3
from PIL import Image
import io

MongoSchema = {
    "team_name": str,
    "num_goals": int,
    "num_wins": int,
    "histogram_win_losses": str
}


class MongoHandler(MongoClient):
    def __init__(self, db_name, collection_name, **kwargs):
        """Initializes the Handler"""
        super(MongoHandler, self).__init__(**kwargs)
        self.db = self[db_name]
        self.collection = self.db[collection_name]

    def post_team(self, data):
        insert = self.collection.insert_one(data)
        return insert

    def display_teams(self):
        team_names = self.list_teams()
        for name in team_names:
            team = Mongo.get_team(name)
            print(team['team_name'])
            print('Goals -', team['num_goals'])
            print('Wins -', team['num_wins'])
    
    def list_teams(self):
        """Returns an iterator of all teams"""
        teams = []
        cur = self.collection.find({})
        done = False
        while not done:
            try:
                teams.append(cur.next()['team_name'])
            except StopIteration:
                done = True
        return teams

    def get_team_iterator(self):
        return self.collection.find({})

    # ex Mongo.get_team('Wolves')
    def get_team(self, team_name):
        """Returns the entry related to a team"""
        team_name = self.collection.find({'team_name': team_name})
        return team_name.next()

    def get_image(self, team_name):
        """Displays a plot for team_name"""
        cursor = self.collection.find({'team_name': team_name})
        team = cursor.next()
        fig = team['histogram_win_losses']
        size = team['im_size']
        return Image.frombytes('RGBA', size, fig)
        

class SQLHandler():
    def __init__(self, name):
        self.connection = sqlite3.connect(name)
        self.cursor = self.connection.cursor()

    def main(self):
        query = """
        SELECT * FROM Unique_Teams
        JOIN Teams_in_Matches
        USING(Unique_Team_ID)
        JOIN Matches
        USING(Match_ID)
        WHERE Season = 2011
        Group By Match_ID
        """
        self.db = pd.DataFrame(self.cursor.execute(query).fetchall())
        self.db.columns = [x[0] for x in self.cursor.description]
        self.teams = self.db['TeamName'].unique()
        list_teams = []
        for team in self.teams:
            list_teams.append(self.main_team(team))
        return list_teams

    def get_home_games(self, team_games, team_name):
        key = {'H': 1, 'A': 0, 'D': .5}
        games = team_games[team_games['HomeTeam'] == team_name]
        wins = games['FTR'].str.match('H').sum()
        loss = games['FTR'].str.match('A').sum()
        draws = games['FTR'].str.match('D').sum()
        games = games.assign(value=games['FTR'].map(key))
        goals = games['FTHG'].sum()
        return games, wins, loss, draws, goals

    def get_away_games(self, team_games, team_name):
        key = {'A': 1, 'H': 0, 'D': .5}
        games = team_games[team_games['AwayTeam'] == team_name]
        wins = games['FTR'].str.match('A').sum()
        loss = games['FTR'].str.match('H').sum()
        draws = games['FTR'].str.match('D').sum()
        games = games.assign(value=games['FTR'].map(key))
        goals = games['FTHG'].sum()
        return games, wins, loss, draws, goals

    def main_team(self, team_name):
        team_games = self.db[self.db['TeamName'] == team_name]
        h_games, h_wins, h_loss, h_draws, h_goals = (
            self.get_home_games(team_games, team_name))
        a_games, a_wins, a_loss, a_draws, a_goals = (
            self.get_away_games(team_games, team_name))
        goals = a_goals + h_goals
        draws = a_draws + h_draws
        losses = a_loss + h_loss
        wins = a_wins + h_loss
        team_games = a_games.append(h_games)
        team_games = team_games.sort_values(by='Date')
        fig, image_size = self.plot(team_name, losses, draws, wins)
        return {
            'team_name': team_name,
            "num_goals": int(goals),
            "num_wins": int(wins),
            "histogram_win_losses": fig,
            "im_size": image_size
        }

    def plot(self, team_name, losses, draws, wins):
        """Returns a bytearray of the plot
           And the size of the image"""
        fig, ax = plt.subplots(figsize=(6, 6))
        to_plot = sorted([(losses, 'Loss', "#006D2C"),
               (draws, 'Draw', "#31A354"),
               (wins, 'Win', "#74C476")],
               key=lambda x: x[0],
               reverse=True)
        for x in to_plot:
            plt.bar(
                x=0,
                height=x[0],
                color=x[2],
                bottom=1,
                edgecolor="#000")
        plt.xticks(ticks=[])
        plt.yticks(ticks=[x[0] for x in to_plot], labels=[
                (str(x[0]) + '-' + str(x[1])) for x in to_plot])
        plt.title('Win-Loss Histogram for %s' % team_name)
        plt.savefig('temp')
        img = Image.open('temp.png', mode='r')
        plt.close('all')
        return img.tobytes(), img.size



def parse_and_post(Mongo, create_db):
    if create_db:
        sql = SQLHandler('database.sqlite')
        list_teams = sql.main()
        for team in list_teams:
            Mongo.post_team(team)

def make_final_image(Mongo):
    real_x = 3000
    real_y = 1800
    real_size = 300
    new_im = Image.new('RGBA', (real_x, real_y))

    def paste(im, i, j):
        im.thumbnail((real_size, real_size))
        new_im.paste(im, (i, j))
        del(im)
    iterator = Mongo.get_team_iterator()
    try:
        for x in range(0, real_x, real_size):
            for y in range(0, real_y, real_size):
                paste(Mongo.get_image(iterator.next()['team_name']), x, y)
    except StopIteration:
        pass
    new_im.save('final.png')
    #new_im.show()

if __name__ == "__main__":
    Mongo = MongoHandler(
        db_name='futbol_db',
        collection_name='futbol_coll',
        port=27017)

    # set to True to create mongodb
    create_db = False
    parse_and_post(Mongo, create_db)

    # Mongo.display_teams()  # displays all teams and info
    # print(Mongo.list_teams())  # displays a list of teams
    # wigan_img = Mongo.get_image('Wigan')  # returns image plot for team

    make_final_image()
    
    

    
    
    
    
    
    
    
    
