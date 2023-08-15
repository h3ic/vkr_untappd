import pandas as pd
import json

def create_users():
    with open('users_raw.json', 'r') as f:
        users = json.load(f)
        print(len(users))

    for i in range(len(users)):
        cur = users[i]['user']
        users[i] = {
            "uid": cur['uid'],
            "user_name": cur['user_name'],
            "location": cur['location'],
            "bio": cur['bio'],
            "first_name": cur['first_name'],
            "last_name": cur['last_name'],
        }

    with open('users_clean.json', 'w') as f:
        json.dump(users, f)


create_users()


merged_df = pd.concat([df1, df2], ignore_index=True)
merged_df.shape

merged_df = merged_df.drop_duplicates(subset='user_name')
merged_df.shape

merged_df.to_csv('users.csv')

# move breweries info out of users_beers.csv

def create_breweries_df():
    filtered_beers_df = users_beers_df.drop_duplicates(subset='brewery_id')
    filtered_checkins_df = checkins_df.drop_duplicates(subset='brewery_id')

    breweries = []
    for _, row in filtered_beers_df.iterrows():
        if (row['brewery_id'] != row['brewery_id']):
            continue
        brewery = {
            "id": int(row['brewery_id']),
            "name": row['brewery_name'],
            "type": row['brewery_type'],
            "country": row['brewery_country'],
            "city": row['brewery_city'],
            "lat": row['brewery_lat'],
            "lng": row['brewery_lng'],
        }
        breweries.append(brewery)

    new_df = pd.DataFrame.from_records(breweries)

    for _, row in filtered_checkins_df.iterrows():
        if (row['brewery_id'] != row['brewery_id'] or new_df['id'].isin([row['brewery_id']]).any()):
            continue
        brewery = {
            "id": int(row['brewery_id']),
            "name": row['brewery_name'],
            "type": row['brewery_type'],
            "country": row['brewery_country'],
            "city": row['brewery_city'],
            "lat": row['brewery_lat'],
            "lng": row['brewery_lng'],
        }
        breweries.append(brewery)

    new_df = pd.DataFrame.from_records(breweries)
    new_df.to_csv('../breweries.csv', index=False)


def create_venues_df():
    checkins_df = pd.read_csv('./data/checkins.csv')
    checkins_df = checkins_df[checkins_df['venue_id'].notna()]
    checkins_df = checkins_df.drop_duplicates(subset='venue_id')

    venues = []
    retail_venues = []

    for _, row in checkins_df.iterrows():
        venue = {
                "id": row['venue_id'],
                "primary_category": row['venue_primary_category'],
                "category": row['venue_category'],
                "country": row['venue_country'],
                "lat": row['venue_lat'],
                "lng": row['venue_lng'],
        }
        retail_venue = {
                "id": row['retail_venue_id'],
                "primary_category": row['retail_venue_primary_category'],
                "category": row['retail_venue_category'],
                "lat": row['retail_venue_lat'],
                "lng": row['retail_venue_lng'],
            }

        venues.append(venue)
        retail_venues.append(retail_venue)

    venues_df = pd.DataFrame.from_records(venues)
    venues_df.to_csv('venues.csv', index=False)

    retail_venues_df = pd.DataFrame.from_records(retail_venues)
    retail_venues_df.to_csv('retail_venues.csv', index=False)
        
def create_retail_venues_df():
    checkins_df = pd.read_csv('./data/checkins.csv')

    checkins_df = checkins_df[checkins_df['retail_venue_id'].notna()]
    checkins_df = checkins_df.drop_duplicates(subset='retail_venue_id')
    retail_venues = []

    for _, row in checkins_df.iterrows():
        retail_venue = {
                "id": row['retail_venue_id'],
                "primary_category": row['retail_venue_primary_category'],
                "category": row['retail_venue_category'],
                "lat": row['retail_venue_lat'],
                "lng": row['retail_venue_lng'],
            }

        retail_venues.append(retail_venue)

    retail_venues_df = pd.DataFrame.from_records(retail_venues)
    retail_venues_df.to_csv('retail_venues.csv', index=False)

def fill_missing_beers_data():
    users_beers_df = pd.read_csv('./data/users_beers.csv')
    users_beers_df = users_beers_df.drop_duplicates(subset='id')

    beers_df = pd.read_csv('./data/beers.csv')

    for i, row in beers_df.iterrows():
        beer_id = row['id']
        beer = users_beers_df[users_beers_df['id'] == beer_id]
        print(beer)
        if beer.shape[0] != 0:
            beers_df.loc[i, 'score'] = beer['beer_score'].iloc[0]

    beers_df.to_csv('beers2.csv', index=False)

def create_beers_df():
    checkins_df = pd.read_csv('./checkins.csv')
    users_beers_df = pd.read_csv('./data/users_beers.csv')

    checkins_df = checkins_df.drop_duplicates(subset='beer_id')
    users_beers_df = users_beers_df.drop_duplicates(subset='id')
    users_beers = []

    for _, row in users_beers_df.iterrows():
        beer = {
            "id": row['id'],
            "name": row['beer_name'],
            "style": row['beer_style'],
            "abv": row['beer_abv'],
            "ibu": row['beer_ibu'],
            "rating_count": row['beer_rating_count'],
            "score": row['beer_score'],
            "brewery_id": row['brewery_id'],
        }
        users_beers.append(beer)

    from_users_beers_df = pd.DataFrame.from_records(users_beers)
    from_users_beers_df = from_users_beers_df.drop_duplicates(subset='id')
    from_users_beers_df.to_csv('from_users_beers.csv', index=False)

    checkins_beers = []
    for _, row in checkins_df.iterrows():
        new_df_row = from_users_beers_df[from_users_beers_df['id']
                                         == row['beer_id']]

        if new_df_row.shape[0] == 0:
            beer = {
                "id": row['id'],
                "name": row['beer_name'],
                "style": row['beer_style'],
                "abv": row['beer_abv'],
                "brewery_id": row['brewery_id'],
            }
            checkins_beers.append(beer)

        elif new_df_row['brewery_id'].isna().any():
            from_users_beers_df.loc[new_df_row.index,
                                    'brewery_id'] = row['brewery_id']

    from_checkins_df = pd.DataFrame.from_records(checkins_beers)

    merged_df = pd.concat(
        [from_users_beers_df, from_checkins_df], ignore_index=True)
    merged_df.to_csv('beers.csv', index=False)