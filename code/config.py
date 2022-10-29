from app import app
from flaskext.mysql import  MySQL

mysql = MySQL()
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'wx_data'  # need to provide the database name here
app.config['MYSQL_HOST'] = 'localhost'
mysql.init_app(app)