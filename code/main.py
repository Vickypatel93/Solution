from app import app
from config import mysql
from flask import jsonify
from flask import flash, request


@app.route('/api/weather/')
def filter_data():
    table_name = 'USC00339312'
    args = request.args
    date =  args.get('date')
    print(args)
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("select *  from " + table_name+ " where _date = %s" %date) # Need to provide the table name here.
        empRows = cursor.fetchall()
        print(empRows)
        response = jsonify(empRows)
        response.status_code = 200
        return response

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/yield/')
def filter_data():
    table_name = 'US_corn_grain_yield'
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("select *  from " + table_name) # Need to provide the table name here.
        empRows = cursor.fetchall()
        print(empRows)
        response = jsonify(empRows)
        response.status_code = 200

        return response

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/yield/')
def filter_data():
    table_name = 'US_corn_grain_yield'
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("select *  from " + table_name) # Need to provide the table name here.
        empRows = cursor.fetchall()
        print(empRows)
        response = jsonify(empRows)
        response.status_code = 200

        return response

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

@app.route('/api/weather/stats')
def filter_data():
    table_name = 'result'
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("select *  from " + table_name) # Need to provide the table name here.
        empRows = cursor.fetchall()
        print(empRows)
        response = jsonify(empRows)
        response.status_code = 200

        return response

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    app.run()
