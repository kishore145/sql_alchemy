import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Create engine to DB

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect Tables into SQLAlchemy ORM
Base = automap_base()
print(Base.classes.keys())

Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB ane
session = Session(engine)


# Setup Flask
app = Flask(__name__)


# Flask Routes

@app.route("/")
def welcome():
    print(Base.classes.keys())
    #session = Session(engine)
    return (
        f"Hawaii Climate Analysis API! - Date format (YYYY-MM-DD)<br/>"
        f"Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/start/end"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the precipitation data for the last year/ since no data is available for last year, extend the criterion by one more year"""
    # create sessions under each route/thread as it gives runtime error when trying to use same sqlalchemy object in different threads
    session = Session(engine)
    # Calculate the date 2 year ago from today
    prev_year = dt.date.today() - dt.timedelta(days=730)

    # Query for the date and precipitation for the last 2 year
    precipitation = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= prev_year).all()

    # Dict with date as the key and prcp as the value
    precip = {date: prcp for date, prcp in precipitation}
    return jsonify(precip)


@app.route("/api/v1.0/stations")
def stations():
    # create sessions under each route/thread as it gives runtime error when trying to use same sqlalchemy object in different threads
    session = Session(engine)
    """Return a list of stations."""
    results = session.query(Station.station).all()

    # Unravel results into a 1D array and convert to a list
    stations = list(np.ravel(results))
    return jsonify(stations)


@app.route("/api/v1.0/tobs")
def temp_monthly():
    # create sessions under each route/thread as it gives runtime error when trying to use same sqlalchemy object in different threads
    session = Session(engine)
    """Return the temperature observations (tobs) for previous year."""
    # Calculate the date 2 year ago from today
    prev_year = dt.date.today() - dt.timedelta(days=730)

    # Query the most active station for all tobs from the last 2 years 
    results = session.query(Measurement.tobs).\
        filter(Measurement.station == 'USC00519397').\
        filter(Measurement.date >= prev_year).all()

    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))

    # Return the results
    return jsonify(temps)


@app.route("/api/v1.0/temp/<start_date>")
@app.route("/api/v1.0/temp/<start_date>/<end_date>")
def stats(start_date=None, end_date=None):
    
    """Return TMIN, TAVG, TMAX."""
    print(start_date, end_date)

    #use same formulae from notebook for calculating the values
    if not end_date:
        # create sessions under each route/thread as it gives runtime error when trying to use same sqlalchemy object in different threads
        session = Session(engine)
        # calculate TMIN, TAVG, TMAX for dates greater than start
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date ).all()
        # Unravel results into a 1D array and convert to a list
        temps = list(np.ravel(results))
        return jsonify(temps)
    else:
        # create sessions under each route/thread as it gives runtime error when trying to use same sqlalchemy object in different threads
        session = Session(engine)
        # calculate TMIN, TAVG, TMAX with start and stop
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
        # Unravel results into a 1D array and convert to a list
        print(results)
        temps = list(np.ravel(results))
        return jsonify(temps)


if __name__ == '__main__':
    app.run()
