"""
Extract school detection with given threshold score to be written in geojson files.

author: nana@developmentseed.org

run:
    python3 extract_school_detection_to_geo.py --csv=outputs_models/PostgresQGIS_csv_exportation/Query_Results_ghana.csv \
            --thresold=0.9 \
            --country=ghana
"""

import os
from os import path as op
import pandas as pd
import json
from geojson import Feature, FeatureCollection
import click

def read_large_csv(csv, c_size = 5000):
    """read large csv
    Dataframe head:
    id predictions geom
    8469951 {"not_school": 0.0045, "school": 0.9956} {"type":"Polygon","coordinates":[[[-1.21673584...
    ---
    Args:
        csv: results from RDS database exported as csv
    Returns:
        df: pandas dataframe
    """
    dfs = [chunck_df for chunck_df in pd.read_csv(csv, chunksize=c_size)]
    df = pd.concat(dfs)
    return df

def extract_schools2geo(df, thresold, country):
    """export school prediction as geojson that higher than given thresold score

    ---
    Args:
        df: pandas dataframe
        thresold: score thresold
        country: country name, e.g. ghana
    """
    df['predictions'] = df.predictions.apply(json.loads)
    df['geom'] = df.geom.apply(json.loads)
    new_items = zip(df['predictions'], df['geom'])
    features = []
    for pred, geo in new_items:
        if pred["school"] >=float(thresold):
            features.append(Feature(geometry=geo, properties=dict(school=pred["school"])))
    feature_collection = FeatureCollection(features)
    with open(f'{country}_{thresold}_schools.geojson', 'w') as results:
        json.dump(feature_collection, results)
    print(f'{country}_{thresold}_schools.geojson saved to {os.getcwd()}')

@click.command(short_help="write geojson from school detection that extract from RDS database")
@click.option('--csv', help='path to the export csv from RDS')
@click.option('--thresold', help='prediction confident score thresold, e.g.0.9')
@click.option('--country', help='country name for the prediction')

def main(csv, thresold, country):
    df = read_large_csv(csv)
    extract_schools2geo(df, thresold, country)

if __name__=="__main__":
    main()
