users_dtype = {
    "id": 'uint32',
    "user_name": 'object',
    "first_name": 'object',
    "last_name": 'object',
    "location": 'object',
    "bio": 'object',
    "total_badges": 'uint32',
    "total_friends": 'uint32',
    "total_checkins": 'uint32',
    "total_beers": 'uint32',
    "total_created_beers": 'uint32',
    "total_photos": 'uint32',
    "date_joined": 'object',
}

users_beers_dtype = {
    "id": 'uint32',
    "user_name": 'object',
    "score": 'float32',
    "beer_name": 'object',
    "beer_style": 'category',
    "beer_abv": 'float32',
    "beer_ibu": 'float32',
    "beer_score": 'float32',
    "beer_rating_count": 'uint32',
    "brewery_id": 'object',
    "count": 'uint16',
    "first_checkin_date": 'object',
    "recent_checkin_date": 'object'
}

beers_dtype = {
    "id": 'uint32',
    "name": 'object',
    "style": 'category',
    "abv": 'float32',
    "ibu": 'float32',
    "beer_score": 'float32',
    "rating_count": 'uint32',
    "brewery_id": 'uint32'
}

checkins_dtype = {
    "id": 'int64',
    "created_at": 'object',
    "comment": 'object',
    "score": 'float16',
    "user_name": 'object',
    "beer_id": 'int64',
    "brewery_id": 'object',
    "language": 'object',
    'container': 'category',
    'venue_id': 'object',
    'retail_venue_id': 'object',
}

breweries_dtypes = {
    "brewery_id": 'uint32',
    "brewery_name": 'object',
    "brewery_type": 'category',
    "brewery_country": 'object',
    "brewery_lat": 'float64',
    "brewery_lng": 'float64',
    "total_beers": 'uint16'
}