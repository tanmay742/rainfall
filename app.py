from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash, check_password_hash
import os

# Generate a random secret key
secret_key = os.urandom(24).hex()
print(secret_key)

app = Flask(__name__)
app.config['SECRET_KEY'] =secret_key  # Change this to a random secret key
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:admin123@localhost/rain'
db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)

# Login route
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = User.query.filter_by(email=email).first()
        if user and check_password_hash(user.password, password):
            session['user_id'] = user.id
            return redirect(url_for('index'))
        else:
            return render_template('login.html', message='Invalid email or password')
    return render_template('login.html')

# Signup route
@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        hashed_password = generate_password_hash(password)
        try:
            new_user = User(username=username, email=email, password=hashed_password)
            db.session.add(new_user)
            db.session.commit()
            return redirect(url_for('login'))
        except IntegrityError:
            db.session.rollback()
            return render_template('signup.html', message='Email or username already exists')
    return render_template('signup.html')

# Logout route
@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('login'))

# Index route
@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    # Define the colors for each rainfall category
    rainfall_colors = {
        "Heavy_Rainfall": "#FFA500",  # Orange
        "Very_Heavy_Rainfall": "#FF0000",  # Red
        "Extremely_Heavy_Rainfall": "#8B0000"  # Dark Red
    }
    return render_template('index.html', rainfall_colors=rainfall_colors)



engine = create_engine('mysql+pymysql://root:admin123@localhost/rain')
# Data route
@app.route('/data', methods=['POST'])
def get_data():
    from_year = int(request.form['from_year'])
    to_year = int(request.form['to_year'])
    from_month = int(request.form['from_month'])
    to_month = int(request.form['to_month'])
    
    print(f"Received request: From Year: {from_year}, To Year: {to_year}, From Month: {from_month}, To Month: {to_month}")
    
    result = execute_queries(engine, from_year, to_year, from_month, to_month)
    # ................................................CODE ADDED 3LINES
    very_high_rainfall_dates, extremely_high_rainfall_dates = get_high_rainfall_dates(from_year, to_year)
    result['very_high_rainfall_dates'] = very_high_rainfall_dates
    result['extremely_high_rainfall_dates'] = extremely_high_rainfall_dates
    print("Sending data back to the frontend:", result)
    
    return jsonify(result)

# Function to transform data
def transform_data(df, from_year, to_year, from_month, to_month):
    transformed_data = []
    for index, row in df.iterrows():
        date = pd.to_datetime(row['Unnamed: 0'])
        for i in range(1, 32):
            if date.year >= from_year and date.year <= to_year and date.month >= from_month and date.month <= to_month:
                transformed_data.append({'Date': date + pd.DateOffset(days=i-14), 'Rainfall': row[i], 'Station': row['DATES']})
    return pd.DataFrame(transformed_data)

# Function to execute queries
def execute_queries(engine, from_year, to_year, from_month, to_month):
    query_total = f"""
        SELECT YEAR(Date) AS Year, MONTH(Date) AS Month,
               COALESCE(SUM(Rainfall), 0) AS Total_Rainfall
        FROM rf_data
        WHERE YEAR(Date) BETWEEN {from_year} AND {to_year}
            AND MONTH(Date) BETWEEN {from_month} AND {to_month}
        GROUP BY YEAR(Date), MONTH(Date)
        ORDER BY Year, Month
    """
    query_days_with_rainfall = f"""
        SELECT YEAR(Date) AS Year, MONTH(Date) AS Month,
               COALESCE(COUNT(*), 0) AS Days_with_Rainfall
        FROM rf_data
        WHERE Rainfall > 0
            AND YEAR(Date) BETWEEN {from_year} AND {to_year}
            AND MONTH(Date) BETWEEN {from_month} AND {to_month}
        GROUP BY YEAR(Date), MONTH(Date)
        ORDER BY Year, Month
    """
    query_rainfall_categories = f"""
        SELECT YEAR(Date) AS Year,
               COALESCE(SUM(CASE WHEN Rainfall >= 64.5 AND Rainfall <= 115.5 THEN 1 ELSE 0 END), 0) AS Heavy_Rainfall,
               COALESCE(SUM(CASE WHEN Rainfall >= 115.6 AND Rainfall <= 204.4 THEN 1 ELSE 0 END), 0) AS Very_Heavy_Rainfall,
               COALESCE(SUM(CASE WHEN Rainfall >= 204.5 THEN 1 ELSE 0 END), 0) AS Extremely_Heavy_Rainfall
        FROM rf_data
        WHERE YEAR(Date) BETWEEN {from_year} AND {to_year}
            AND MONTH(Date) BETWEEN {from_month} AND {to_month}
        GROUP BY YEAR(Date)
        ORDER BY Year
    """
  
    # query_95th_percentile = f"""
    # SET @rownum := 0;

    # SELECT Date, Rainfall
    # FROM (
    #     SELECT Date,
    #         Rainfall,
    #         @rownum := @rownum + 1 AS rownum,
    #         total_count
    #     FROM (
    #         SELECT Date, Rainfall
    #         FROM rf_data
    #         WHERE YEAR(Date) BETWEEN {from_year} AND {to_year}
    #         AND MONTH(Date) BETWEEN {from_month} AND {to_month}
    #         ORDER BY Rainfall
    #     ) AS sorted_data,
    #     (SELECT @rownum := 0) AS r,
    #     (SELECT COUNT(*) AS total_count FROM rf_data WHERE YEAR(Date) BETWEEN {from_year} AND {to_year} AND MONTH(Date) BETWEEN {from_month} AND {to_month}) AS t
    # ) AS ranked_data
    # WHERE rownum = FLOOR(0.95 * total_count) + 1;
    # """





    conn = engine.connect()
    query_results_total = pd.read_sql(query_total, conn)
    query_results_days_with_rainfall = pd.read_sql(query_days_with_rainfall, conn)
    query_results_rainfall_categories = pd.read_sql(query_rainfall_categories, conn)
    # query_results_95th_percentile = pd.read_sql(query_95th_percentile, conn)
    conn.close()
    
    result = {
        'total_rainfall': query_results_total.to_dict(orient='records'),
        'days_with_rainfall': query_results_days_with_rainfall.to_dict(orient='records'),
        'rainfall_categories': query_results_rainfall_categories.to_dict(orient='records'),
        # '95th_percentile': query_results_95th_percentile.to_dict(orient='records')
    }
    
    return result


# Function to get high rainfall dates
def get_high_rainfall_dates(from_year, to_year):
    query_high_rainfall_dates = f"""
        SELECT YEAR(Date) AS Year, Rainfall, GROUP_CONCAT(Date) AS Dates
        FROM rf_data
        WHERE Rainfall >= 115.6 AND Rainfall <= 204.4
            AND YEAR(Date) BETWEEN {from_year} AND {to_year}
        GROUP BY YEAR(Date), Rainfall
    """
    query_extreme_rainfall_dates = f"""
        SELECT YEAR(Date) AS Year, Rainfall, GROUP_CONCAT(Date) AS Dates
        FROM rf_data
        WHERE Rainfall >= 204.5
            AND YEAR(Date) BETWEEN {from_year} AND {to_year}
        GROUP BY YEAR(Date), Rainfall
    """
    conn = engine.connect()
    high_rainfall_dates = pd.read_sql(query_high_rainfall_dates, conn)
    extreme_rainfall_dates = pd.read_sql(query_extreme_rainfall_dates, conn)
    conn.close()

    very_high_rainfall_dates = {}
    extremely_high_rainfall_dates = {}

    for index, row in high_rainfall_dates.iterrows():
        year = row['Year']
        if year not in very_high_rainfall_dates:
            very_high_rainfall_dates[year] = []
        dates = row['Dates'].split(',')
        very_high_rainfall_dates[year].extend(dates)

    for index, row in extreme_rainfall_dates.iterrows():
        year = row['Year']
        if year not in extremely_high_rainfall_dates:
            extremely_high_rainfall_dates[year] = []
        dates = row['Dates'].split(',')
        extremely_high_rainfall_dates[year].extend(dates)

    for year in very_high_rainfall_dates:
        very_high_rainfall_dates[year] = [str(pd.to_datetime(date)) for date in very_high_rainfall_dates[year]]

    for year in extremely_high_rainfall_dates:
        extremely_high_rainfall_dates[year] = [str(pd.to_datetime(date)) for date in extremely_high_rainfall_dates[year]]

    return very_high_rainfall_dates, extremely_high_rainfall_dates





# this code is for date to date range form (FORM-2)...
@app.route('/date-range', methods=['POST'])
def get_date_range_data():
    # Check if the form fields 'from_date' and 'to_date' are present in the request
    if 'from_date' in request.form and 'to_date' in request.form:
        # Retrieve the values of 'from_date' and 'to_date' from the form data
        from_date = request.form['from_date']
        to_date = request.form['to_date']
        
        print(f"Received request: From Date: {from_date}, To Date: {to_date}")
        
        # Call the function to execute the query for the date range
        result = execute_date_range_query(engine, from_date, to_date)
        
        # Additional code to calculate 95th percentile for each day and month
        start_year, start_month, start_day = map(int, from_date.split('-'))
        end_year, end_month, end_day = map(int, to_date.split('-'))
        
        percentile_result = {}
        # Loop through each day and month combination between start_date and end_date
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    break
                
                for day in range(1, 32):
                    if year == start_year and month == start_month and day < start_day:
                        continue
                    if year == end_year and month == end_month and day > end_day:
                        break
                    
                    # Calculate the 95th percentile for the current day and month
                    percentile_data = calculate_percentile_for_day_month(day, month, start_year, end_year)
                    if percentile_data:
                        percentile_result[f"{month:02d}-{day:02d}"] = percentile_data


        # Create a combined response with both results
        combined_response = {
            "result": result,
            "percentile_result": percentile_result
        }

        # Sending data back to the frontend
        print("Sending data back to the frontend:", combined_response)

        # Return the combined result as JSON
        return jsonify(combined_response)
    else:
        # If the required form fields are not present, return a 400 Bad Request error
        return jsonify({'error': 'Missing form fields'}), 400


import numpy as np
def calculate_percentile_for_day_month(day, month, start_year, end_year):
    query = f"""
        SELECT Date, Rainfall
        FROM rf_data
        WHERE DAY(Date) = {day} AND MONTH(Date) = {month} AND YEAR(Date) BETWEEN {start_year} AND {end_year}
        ORDER BY Rainfall;
    """
    # print("SQL Query:", query)  # Debugging statement
    conn = engine.connect()
    query_result = pd.read_sql(query, conn)
    conn.close()
    
    # print("Query Result:")
    # print(query_result)  # Debugging statement

    if not query_result.empty:
        # Drop NaN values from the Rainfall column
        query_result = query_result.dropna(subset=['Rainfall'])
        
        if not query_result.empty:
            # Convert Rainfall column to float type
            query_result['Rainfall'] = query_result['Rainfall'].astype(float)
            
            # Calculate the 95th percentile
            percentile_95th = np.percentile(query_result['Rainfall'], 95)
            
            # Check for NaN and convert to None
            if np.isnan(percentile_95th):
                percentile_95th = None
            # print("Percentile Value:", percentile_95th)  # Debugging statement
            return {'day': day, 'month': month, 'percentile_95th': percentile_95th}
        else:
            # print("No valid rainfall data found for the query.")
            return {'day': day, 'month': month, 'percentile_95th': None}
    else:
        # print("No data found for the query.")
        return {'day': day, 'month': month, 'percentile_95th': None}
  


# Function to execute query for date range
def execute_date_range_query(engine, from_date, to_date):
    query_date_range = f"""
        SELECT Date, Rainfall
        FROM rf_data
        WHERE Date BETWEEN '{from_date}' AND '{to_date}'
        ORDER BY Date
    """
    conn = engine.connect()
    query_results = pd.read_sql(query_date_range, conn)
    conn.close()
    
    # Replace NaN values in the Rainfall column with 0
    query_results['Rainfall'] = query_results['Rainfall'].fillna(0)
    
    result = {
        'datewise_rainfall': query_results.to_dict(orient='records')
    }
    
    # print("Result:")
    # print(result)  # Print the result before returning
    
    return result


if __name__ == '__main__':
    app.run(debug=True)



 
# EXTRA CODES IN CASE OF...

 # Function to calculate 95th percentile for a specific day and month for form2(date to date form)
# def calculate_percentile_for_day_month(day, month,start_year,end_year):
#     query_95th_percentile = f"""
#         SET @rownum := 0;

#         SELECT Date, Rainfall
#         FROM (
#             SELECT Date,
#                 Rainfall,
#                 @rownum := @rownum + 1 AS rownum,
#                 total_count
#             FROM (
#                 SELECT Date, Rainfall
#                 FROM rf_data
#                 WHERE DAY(Date) = {day} AND MONTH(Date) = {month} AND YEAR(Date)>={start_year} AND YEAR(Date)<={end_year}
#                 ORDER BY Rainfall
#             ) AS sorted_data,
#             (SELECT @rownum := 0) AS r,
#             (SELECT COUNT(*) AS total_count FROM rf_data WHERE DAY(Date) = {day} AND MONTH(Date) = {month} AND YEAR(Date)>={start_year} AND YEAR(Date)<={end_year}) AS t
#         ) AS ranked_data
#         WHERE rownum = FLOOR(0.95 * total_count) + 1;
#     """






# CODE 2 USED IN GET_RANGE _dATA FUNCTION
        # percentile_result = {}
        # for year in range(start_year, end_year + 1):
        #     for month in range(1, 13):  # Loop through all months in the year
        #         # Skip months before start_date in the first year and after end_date in the last year
        #         if year == start_year and month < start_month:
        #             continue
        #         if year == end_year and month > end_month:
        #             break
                
            
                
        #         for day in range(1, 32):  # Assume maximum 31 days in a month
        #             # Skip days before start_date in the first month and after end_date in the last month
        #             if year == start_year and month == start_month and day < start_day:
        #                 continue
        #             if year == end_year and month == end_month and day > end_day:
        #                 break
                    
        #             # Calculate the 95th percentile for the current day and month
        #             percentile_data = calculate_percentile_for_day_month(day, month)
        #             percentile_result[f"{year}-{month:02d}-{day:02d}"] = percentile_data

                # Merge the original result and percentile result
        #     result.update(percentile_result)
            
        #     # Return the combined result as JSON
        #     return jsonify(result)
        # else:
        #     # If the required form fields are not present, return a 400 Bad Request error
        #     return jsonify({'error': 'Missing form fields'}), 400