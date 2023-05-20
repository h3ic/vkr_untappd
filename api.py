import json
import sys
import pandas as pd
import requests
from config import KEYRING

API_LINK = "https://api.untappd.com/v4/"
USER_METHOD = "user/friends/"
USER_INFO_METHOD = "user/info/"
BEER_INFO_METHOD = "beer/info/"  # 16630
BEER_CHECKINS_METHOD = "beer/checkins/"  # 1263559
USER_CHECKINS_METHOD = "user/checkins/"
USER_BEERS_METHOD = "user/beers/"
BREWERY_INFO_METHOD = "brewery/info/"
BREWERY_BEER_LIST_METHOD = "brewery/beer_list/"

try:
    KEYRING_INDEX = sys.argv[1]
except:  # IndexError
    KEYRING_INDEX = '33'

TOKENS = f"?client_id={KEYRING[KEYRING_INDEX]['client_id']}&client_secret={KEYRING[KEYRING_INDEX]['client_secret']}"


def get_users(username):
    with open('users_raw.json', 'r') as f:
        users = json.load(f)
    try:
        for i in range(0, 11128, 25):
            resp = requests.get(API_LINK + USER_METHOD +
                                username + TOKENS + f'&offset={i}')
            resp = resp.json()
            print(resp)
            users.extend(resp['response']['items'])
    except:
        print('err')
    finally:
        print('======================\n', len(users))
        with open('users_raw.json', 'w') as f:
            json.dump(users, f)


def get_users_info():
    with open('offset.json', 'r') as f:
        file = json.load(f)
        prev_offset = int(file['offset'])
    with open('users_info.json', 'r') as f:
        users_info = json.load(f)

    offset = prev_offset + 1
    df = pd.read_csv('users.csv')
    df = df[offset:]
    private_count = 0
    new_users_info = []

    try:
        for _, row in df.iterrows():
            user_name = row['user_name']
            resp = requests.get(
                API_LINK + USER_INFO_METHOD + f'{user_name}' + TOKENS + '&compact=true')
            resp = resp.json()
            user = resp['response']['user']
            if (user['is_private']):
                private_count += 1
                continue
            info = {
                "id": user['id'],
                "user_name": user['user_name'],
                "first_name": user['first_name'],
                "last_name": user['last_name'],
                "location": user['location'],
                "bio": user['bio'],
                "total_badges": user['stats']['total_badges'],
                "total_friends": user['stats']['total_friends'],
                "total_checkins": user['stats']['total_checkins'],
                "total_beers": user['stats']['total_beers'],
                "total_created_beers": user['stats']['total_created_beers'],
                "total_photos": user['stats']['total_photos'],
                "date_joined": user['date_joined']
            }
            new_users_info.append(info)
    except:
        print('err')
    finally:
        users_info.extend(new_users_info)
        with open('users_info.json', 'w') as f:
            json.dump(users_info, f)
        with open('offset.json', 'w') as f:
            json.dump({"offset": prev_offset +
                      private_count + len(new_users_info)}, f)


def get_users_checkins():
    new_users_checkins = []
    users_df = pd.read_csv('users.csv')
    checkins_df = pd.read_csv('users_checkins.csv')

    try:
        for _, row in users_df.iterrows():
            user_name = row['user_name']
            if (checkins_df['user_name'].eq(user_name)).any():
                continue

            cur_user_checkins = []
            max_id = None

            while True:
                if max_id:
                    resp = requests.get(
                        API_LINK + USER_CHECKINS_METHOD + f'{user_name}' + TOKENS + f'&limit=100&max_id={max_id}')
                else:
                    resp = requests.get(
                        API_LINK + USER_CHECKINS_METHOD + f'{user_name}' + TOKENS + f'&limit=100')

                resp = resp.json()
                print(resp)

                if (resp['meta']['code'] == 404):
                    break
                if ('checkins' not in resp['response'] or resp['response']['checkins']['items'] == []):
                    break

                checkins = resp['response']['checkins']['items']

                # stop at last 'page'
                if checkins == []:
                    break

                max_id = resp['response']['pagination']['max_id']

                for checkin in checkins:
                    compact_checkin = {
                        "id": checkin['checkin_id'],
                        "created_at": checkin['created_at'],
                        "comment": checkin['checkin_comment'],
                        "score": checkin['rating_score'],
                        "user_name": checkin['user']['user_name'],
                        "user_location": checkin['user']['location'],
                        "beer_id": checkin['beer']['bid'],
                        "beer_name": checkin['beer']['beer_name'],
                        "beer_style": checkin['beer']['beer_style'],
                        "beer_abv": checkin['beer']['beer_abv'],
                        "beer_active": checkin['beer']['beer_active'],
                        "brewery_id": checkin['brewery']['brewery_id'],
                        "brewery_name": checkin['brewery']['brewery_name'],
                        "brewery_type": checkin['brewery']['brewery_type'],
                        "brewery_country": checkin['brewery']['country_name'],
                        "brewery_lat": checkin['brewery']['location']['lat'],
                        "brewery_lng": checkin['brewery']['location']['lng'],
                        "language": checkin['lang']['text'],
                    }
                    if (checkin['serving_types'] != []):
                        compact_checkin['container'] = checkin['serving_types']['container_id']
                    else:
                        compact_checkin['container'] = None

                    if (checkin['venue'] != []):
                        compact_checkin['venue_id'] = checkin['venue']['venue_id']
                        compact_checkin["venue_lat"] = checkin['venue']['location']['lat']
                        compact_checkin["venue_lng"] = checkin['venue']['location']['lng']
                        compact_checkin["venue_country"] = checkin['venue']['location']['venue_country']
                        compact_checkin["venue_primary_category"] = checkin['venue']['primary_category']
                        if (checkin['venue']['categories']['items'] != []):
                            compact_checkin["venue_category"] = checkin['venue']['categories']['items'][0]['category_name']
                    else:
                        compact_checkin['venue_id'] = None
                        compact_checkin["venue_lat"] = None
                        compact_checkin["venue_lng"] = None
                        compact_checkin["venue_primary_category"] = None
                        compact_checkin["venue_category"] = None

                    if (checkin['retail_venue'] != []):
                        compact_checkin['retail_venue_id'] = checkin['retail_venue']['venue_id']
                        compact_checkin["retail_venue_lat"] = checkin['retail_venue']['location']['lat']
                        compact_checkin["retail_venue_lng"] = checkin['retail_venue']['location']['lng']
                        compact_checkin["retail_venue_primary_category"] = checkin['retail_venue']['primary_category']
                        if (checkin['retail_venue']['categories']['items'] != []):
                            compact_checkin["retail_venue_category"] = checkin['retail_venue']['categories']['items'][0]['category_name']
                    else:
                        compact_checkin['retail_venue_id'] = None
                        compact_checkin["retail_venue_lat"] = None
                        compact_checkin["retail_venue_lng"] = None
                        compact_checkin["retail_venue_primary_category"] = None
                        compact_checkin["retail_venue_category"] = None

                    cur_user_checkins.append(compact_checkin)

            new_users_checkins.extend(cur_user_checkins)
            user_offset += 1

            if (len(new_users_checkins) > 100000):
                new_df = pd.DataFrame.from_records(new_users_checkins)
                merged_df = pd.concat([checkins_df, new_df], ignore_index=True)
                merged_df.to_csv('users_checkins.csv', index=False)
                del new_df
                checkins_df = merged_df

                new_users_checkins = []
    finally:
        checkins_df = pd.read_csv('users_checkins.csv')
        new_users_checkins.extend(cur_user_checkins)
        new_df = pd.DataFrame.from_records(new_users_checkins)
        merged_df = pd.concat([checkins_df, new_df], ignore_index=True)
        merged_df.to_csv('users_checkins.csv', index=False)

        with open('checkins_offset.json', 'w') as f:
            json.dump({"user_offset": user_offset}, f)


def get_users_beers():
    new_users_beers = []
    users_df = pd.read_csv('./data/users.csv')
    beers_df = pd.read_csv('./users_beers2.csv')

    beers_users = beers_df['user_name'].drop_duplicates()
    users_df = users_df[~users_df['user_name'].isin(beers_users)]

    try:
        for _, row in users_df.iterrows():
            user_name = row['user_name']
            total_beers = row['total_beers']

            cur_user_beers = []

            for i in range(0, total_beers, 50):
                resp = requests.get(
                    API_LINK + USER_BEERS_METHOD + f'{user_name}' + TOKENS + f'&offset={i}&limit=50')

                resp = resp.json()
                print(resp)

                if 'beers' not in resp['response'] or resp['response']['beers']['items'] == [] or resp['meta']['code'] == 404:
                    break

                beers = resp['response']['beers']['items']

                # stop at last 'page'
                if beers == []:
                    break

                for beer in beers:
                    compact_beer = {
                        "id": beer['beer']['bid'],
                        "user_name": user_name,
                        "score": beer['rating_score'],
                        "count": beer['count'],
                        "first_checkin_date": beer['first_created_at'],
                        "recent_checkin_date": beer['recent_created_at'],

                        "beer_name": beer['beer']['beer_name'],
                        "beer_style": beer['beer']['beer_style'],
                        "beer_abv": beer['beer']['beer_abv'],
                        "beer_ibu": beer['beer']['beer_ibu'],
                        "beer_score": beer['beer']['rating_score'],
                        "beer_rating_count": beer['beer']['rating_count'],
                        'created_at': beer['beer']['created_at']
                    }

                    if (beer['brewery'] != []):
                        compact_beer["brewery_id"] = beer['brewery']['brewery_id']
                        compact_beer["brewery_name"] = beer['brewery']['brewery_name']
                        compact_beer["brewery_type"] = beer['brewery']['brewery_type']
                        compact_beer["brewery_country"] = beer['brewery']['country_name']
                        compact_beer["brewery_city"] = beer['brewery']['location']['brewery_city']
                        compact_beer["brewery_lat"] = beer['brewery']['location']['lat']
                        compact_beer["brewery_lng"] = beer['brewery']['location']['lng']

                    cur_user_beers.append(compact_beer)

            new_users_beers.extend(cur_user_beers)

            if (len(new_users_beers) > 200000):
                new_df = pd.DataFrame.from_records(new_users_beers)
                merged_df = pd.concat([beers_df, new_df], ignore_index=True)
                merged_df.to_csv('users_beers2.csv', index=False)
                del new_df
                beers_df = merged_df

                new_users_beers = []
    finally:
        beers_df = pd.read_csv('users_beers2.csv')
        new_users_beers.extend(cur_user_beers)
        new_df = pd.DataFrame.from_records(new_users_beers)
        merged_df = pd.concat([beers_df, new_df], ignore_index=True)
        merged_df.to_csv('users_beers2.csv', index=False)


def get_beer(beer_id):
    resp = requests.get(API_LINK + BEER_INFO_METHOD + f'{beer_id}' + TOKENS)
    resp = resp.json()
    print(resp)

    with open('beer.json', 'w') as f:
        json.dump(resp['response']['beer'], f)


def get_beer_checkins(beer_id):
    checkins = []
    try:
        for i in range(0, 8034, 25):
            resp = requests.get(
                API_LINK + BEER_CHECKINS_METHOD + f'{beer_id}' + TOKENS + f'&offset={i}')
            resp = resp.json()
            print(resp, f'i\n')
            checkins.extend(resp['response']['checkins']['items'])
    except:
        print('err')
    finally:
        with open('ipa_checkins.json', 'w') as f:
            json.dump(checkins, f)


def get_breweries_beers():
    breweries_df = pd.read_csv('breweries.csv')
    breweries2_df = pd.read_csv('breweries2.csv')
    beers_df = pd.read_csv('beers.csv')

    breweries_df = breweries_df[~breweries_df['id'].isin(breweries2_df['id'])]

    new_beers = []
    new_breweries = []

    try:
        for _, row in breweries_df.iterrows():
            brewery_id = row['id']

            resp = requests.get(
                API_LINK + BREWERY_INFO_METHOD + f'{brewery_id}' + TOKENS)

            resp = resp.json()
            print(resp)

            brewery = resp['brewery']

            brewery_info = {
                "id": brewery['brewery_id'],
                "name": brewery['brewery_name'],
                "type": brewery['brewery_type'],
                "type_id": brewery['brewery_type_id'],
                "country": brewery['country_name'],
                "beer_count": brewery['beer_count'],
            }

            if (brewery['location'] and brewery['location'] != []):
                brewery_info['city'] = resp['location']['brewery_city']
                brewery_info['lat'] = resp['location']['lat']
                brewery_info['lng'] = resp['location']['lng']

            if (brewery['rating'] and brewery['rating'] != []):
                brewery_info['count'] = resp['rating']['count']
                brewery_info['rating_score'] = resp['rating']['rating_score']

            if (brewery['stats'] and brewery['stats'] != []):
                brewery_info['total_count'] = resp['stats']['total_count']
                brewery_info['unique_count'] = resp['stats']['unique_count']
                brewery_info['monthly_count'] = resp['stats']['monthly_count']
                brewery_info['weekly_count'] = resp['stats']['weekly_count']
                brewery_info['age_on_service'] = resp['stats']['weekly_count']

            new_breweries.append(brewery_info)

            if (brewery['beer_list'] == [] or not brewery['beer_list']):
                continue

            if (brewery['beer_list'][''] == [] or not brewery['beer_list']):
                continue

            for beer in resp['beer_list']:
                new_beer = {
                    "id": beer['beer']['bid'],
                    "score": beer['rating_score'],
                    "count": beer['count'],
                    "brewery_id": brewery_id,

                    "beer_name": beer['beer']['beer_name'],
                    "beer_style": beer['beer']['beer_style'],
                    "beer_abv": beer['beer']['beer_abv'],
                    "beer_ibu": beer['beer']['beer_ibu'],
                    "beer_score": beer['beer']['rating_score'],
                    "beer_rating_count": beer['beer']['rating_count'],
                }
                new_beers.append(new_beer)

            if (len(new_breweries) > 5000):
                new_df = pd.DataFrame.from_records(new_breweries)
                merged_df = pd.concat(
                    [breweries_df, new_df], ignore_index=True)
                merged_df.to_csv('breweries2.csv', index=False)
                del new_df
                breweries_df = merged_df
                new_breweries = []

            if (len(new_beers) > 10000):
                new_df = pd.DataFrame.from_records(new_beers)
                merged_df = pd.concat([beers_df, new_df], ignore_index=True)
                merged_df.to_csv('beers.csv', index=False)
                del new_df
                beers_df = merged_df
                new_beers = []
    finally:
        beers_df = pd.read_csv('beers.csv')
        new_df = pd.DataFrame.from_records(new_beers)
        merged_df = pd.concat([beers_df, new_df], ignore_index=True)
        merged_df.to_csv('users_beers.csv', index=False)


def fix_breweries_info():
    """
    1. create new beers_df
    2. create new breweries_df
    3. remove data from users_beers_df; leave beer_id, username, score, count, first_checkin_date, recent_checkin_date
    """
    beers_df = pd.read_csv('beers.csv')
    breweries_df = pd.read_csv('breweries.csv')
    # users_beers_df = pd.read_csv('./data/users_beers.csv', dtype=users_beers_dtype)
    users_beers_df = pd.read_csv(
        'test_users_beers.csv', dtype=users_beers_dtype)

    users_beers_df = users_beers_df.drop_duplicates(subset='id')
    # users_beers_df = users_beers_df[~users_beers_df['brewery_id'].isin(
    #     breweries_df['id'])]
    users_beers_df = users_beers_df[~users_beers_df['id'].isin(
        beers_df['id'])]

    new_breweries = []
    new_beers = []

    try:
        for _, row in users_beers_df.iterrows():
            beer_id = row['id']
            # time.sleep(1.6)
            resp = requests.get(
                API_LINK + BEER_INFO_METHOD + f'{beer_id}' + TOKENS + '&compact=true')
            print('b4 json()', resp.headers)
            resp = resp.json()
            print(resp)
            beer = resp['response']['beer']

            if (beer['brewery'] and beer['brewery'] != []):
                brewery = beer['brewery']
                brewery_info = {
                    "id": brewery['brewery_id'],
                    "name": brewery['brewery_name'],
                    "type": brewery['brewery_type'],
                    "country": brewery['country_name'],
                }
                if (brewery['location'] and brewery['location'] != []):
                    brewery_info['city'] = brewery['location']['brewery_city']
                    brewery_info['lat'] = brewery['location']['lat']
                    brewery_info['lng'] = brewery['location']['lng']

                new_breweries.append(brewery_info)

            beer_info = {
                "id": beer['bid'],
                "name": beer['beer_name'],
                "abv": beer['beer_abv'],
                "ibu": beer['beer_ibu'],

                "description": beer['beer_description'],
                "is_in_production": beer['is_in_production'],
                "is_homebrew": beer['is_homebrew'],
                "created_at": beer['created_at'],

                "style": beer['beer_style'],
                "rating_count": beer['rating_count'],
                "rating_score": beer['rating_score'],
            }

            if (beer['brewery'] and beer['brewery'] != []):
                beer_info['brewery_id'] = beer['brewery']['brewery_id']

            if (beer['stats'] and beer['stats'] != []):
                stats = beer['stats']
                beer_info['total_count'] = stats['total_count']
                beer_info['monthly_count'] = stats['monthly_count']
                beer_info['total_user_count'] = stats['total_user_count']

                new_beers.append(beer_info)

            if (len(new_breweries) > 5000):
                new_df = pd.DataFrame.from_records(new_breweries)
                merged_df = pd.concat(
                    [breweries_df, new_df], ignore_index=True)
                merged_df.to_csv('breweries.csv', index=False)
                del new_df
                breweries_df = merged_df
                new_breweries = []

            if (len(new_beers) > 50000):
                new_df = pd.DataFrame.from_records(new_beers)
                merged_df = pd.concat([beers_df, new_df], ignore_index=True)
                merged_df.to_csv('beers.csv', index=False)
                del new_df
                beers_df = merged_df
                new_beers = []
    finally:
        breweries_df = pd.read_csv('breweries.csv')
        new_df = pd.DataFrame.from_records(new_breweries)
        merged_df = pd.concat([breweries_df, new_df], ignore_index=True)
        merged_df.to_csv('breweries.csv', index=False)

        beers_df = pd.read_csv('beers.csv')
        new_df = pd.DataFrame.from_records(new_beers)
        merged_df = pd.concat([beers_df, new_df], ignore_index=True)
        merged_df.to_csv('users_beers.csv', index=False)


def get_breweries_beers():
    breweries_df_orig = pd.read_csv('./breweries_with_count.csv')
    beers_df = pd.read_csv('./data/beers.csv')
    new_beers_df = pd.read_csv('./new_beers.csv')
    breweries_df = breweries_df_orig[breweries_df_orig.total_beers.isna()]
    beer_list = []

    try:
        for index, row in breweries_df.iterrows():
            brewery_id = str(row.id)
            print(brewery_id)
            resp = requests.get(
                API_LINK + BREWERY_BEER_LIST_METHOD + brewery_id + TOKENS + '&limit=100')
            resp = resp.json()
            print(resp)
            resp = resp['response']

            if not resp or resp == []:
                continue

            total_count = resp['total_count']
            breweries_df_orig.at[index, 'total_beers'] = total_count

            if total_count == 0 or resp['beers']['items'] == []:
                continue

            for item in resp['beers']['items']:
                beer = item['beer']
                if (beer['bid'] in beers_df.id):
                    continue
                beer = {
                    "id": beer['bid'],
                    "name": beer['beer_name'],
                    "style": beer['beer_style'],
                    "abv": beer['beer_abv'],
                    "ibu": beer['beer_ibu'],
                    "created_at": beer['created_at'],
                    "rating_score": beer['rating_score'],
                    "rating_count": beer['rating_count'],
                    "brewery_id": brewery_id
                }
                beer_list.append(beer)

            for i in range(100, total_count, 100):
                print('offset:', i)
                resp = requests.get(
                    API_LINK + BREWERY_BEER_LIST_METHOD + brewery_id + TOKENS + f'&offset={i}&limit=100')
                resp = resp.json()
                print(resp)
                resp = resp['response']

                if not resp or resp == [] or resp['beers']['items'] == []:
                    continue

                for item in resp['beers']['items']:
                    beer = item['beer']
                    if (beer['bid'] in beers_df.id):
                        continue
                    beer = {
                        "id": beer['bid'],
                        "name": beer['beer_name'],
                        "style": beer['beer_style'],
                        "abv": beer['beer_abv'],
                        "ibu": beer['beer_ibu'],
                        "created_at": beer['created_at'],
                        "rating_score": beer['rating_score'],
                        "rating_count": beer['rating_count'],
                        "brewery_id": brewery_id
                    }
                    beer_list.append(beer)
    finally:
        breweries_df_orig.to_csv('breweries_with_count.csv', index=False)
        new_df = pd.DataFrame.from_records(beer_list)
        merged_df = pd.concat([new_beers_df, new_df], ignore_index=True)
        merged_df.to_csv('new_beers.csv', index=False)


# resp = requests.get(API_LINK + 'brewery/beer_list/' + '38368' + TOKENS + '&offset=50&limit=1000')
# resp = resp.json()

# with open('beers_list2.json', 'w') as f:
#     json.dump(resp, f)