import requests
import csv
import json


def fetch_data():

    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    headers = {
        'x-rapidapi-host': 'community-open-weather-map.p.rapidapi.com',
        'x-rapidapi-key': '9cfca117aamsh3414264bc0e7e99p153e0ejsn9d1bd0ce9bc5'
    }

    indian_states = ["Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Delhi", "Goa",
              "Gujarat", "Haryana"]

    weather_details = []

    for state in indian_states:
        querystring = {"q": f"{state},india", "lat": "0", "lon": "0", "id": "2172797", "lang": "null",
                       "units": "imperial", "mode": "JSON"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()
        print(data)
        # json_object = json.loads(response.text)
        # json_formatted_str = json.dumps(json_object, indent=2)
        # print(json_formatted_str)
        try:
            list_item = [data["name"], data["weather"][0]["description"], data["main"]["temp"], data["main"]["feels_like"],
                         data["main"]["temp_min"], data["main"]["temp_max"], data["main"]["humidity"],
                         data["clouds"]["all"]]
            weather_details.append(list_item)
        except:
            print("api request limit exceeds")

    header = ['State', 'Description', 'Temperature', 'Feels_Like_Temperature', 'Min_Temperature', 'Max_Temperature',
                  'Humidity', 'Clouds']
    path = "/usr/local/airflow/store_files_airflow"
    with open(path + '/Weather_Data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(weather_details)
