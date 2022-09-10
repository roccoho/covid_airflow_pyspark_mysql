import mysql.connector
import sqlalchemy

class MySql:
    def __init__(self, host, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.url = f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}'
        self.engine = sqlalchemy.create_engine(self.url)
        self.mycursor = self.mydb.cursor()


    def get_result(self, to_print=False, title=''):
        result = self.mycursor.fetchall()
        if to_print:
            print('\n')
            if title:
                print(f'{title}')

            for x in result:
                print(x)

        return result
