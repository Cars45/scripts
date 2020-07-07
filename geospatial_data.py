import pandas as pd
import psycopg2
from sqlalchemy import create_engine 

def data_to_db(df, tablename):
    """
        push the data to postgres
    """
    sql_engine = create_engine(
        'postgresql+psycopg2://dami_s:2CsdwGamkqx20@104.197.243.5/cars45db')
    db_conn = sql_engine.connect()
    try:
        df.to_sql(tablename, db_conn, if_exists='replace', method='multi', schema = 'geospatial_data', chunksize=5000)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("records succesfully added to database.")
    finally:
        db_conn.close()

def geoserver_data(url, tablename1):
    data = pd.read_csv(url)
    output = data_to_db(data, tablename1)
    print(output)

#Ward_boundaries
geoserver_data('https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_wards&outputFormat=csv', 'ward_boundary')

#lga_boundaries
geoserver_data('https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_local_government_areas&outputFormat=csv', 'lga_boundary')

#state_boundaries
geoserver_data('https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_states&outputFormat=csv', 'state_boundary')

#Industrial Sites
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_factory_sites&outputFormat=csv", "industial_site")

#Petrol Station Data
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_fuel_stations&outputFormat=csv", "petrol_station")

#Market Location Data
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_markets&outputFormat=csv", "market_location")

#school location data
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_schools&outputFormat=csv", "school_location")

#Religious Centers
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_religious_institutions&outputFormat=csv", "religious_center")

#Post Office Data
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_post_offices&outputFormat=csv", "post_office")

#Government Building 
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_government_buildings&outputFormat=csv", "govt_building")

#Public water supplies
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_public_water_supplies&outputFormat=csv", "public_water")

#Pharmacy
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_pharmacies&outputFormat=csv", "pharmacy_location")

#Health facility
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_health_facilities&outputFormat=csv", "health_facility")

#Settlement Population Estimates
#geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_settlement_pop_estimate&outputFormat=csv", 'settlement_pop_est')

#Settlement Point Population Estimate
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_settlement_points_pop_estimate&outputFormat=csv", 'settlement_point_pop_est')

#Ward Population Estimate
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_ward_pop_estimate&outputFormat=csv", 'ward_pop_est')

#LGA Population Estimates 
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_local_government_pop_estimate&outputFormat=csv", 'lga_pop_est')

#State population estimates 
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_state_pop_estimate&outputFormat=csv", 'state_pop_est')

#Settlement Point
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:sv_settlements&outputFormat=csv", 'settlement_point')

#Settlement Area
geoserver_data("https://gis-a.ie.ehealthafrica.org/geoserver/eHA_db/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=eHA_db:settlement_areas&outputFormat=csv", "settlement_area")
    
