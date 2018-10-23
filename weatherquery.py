from __future__ import print_function

import requests
import redis
import pymysql

print('Loading function')

apikey = "mxHDufXzNoqATQJT6xGEoxajC6fhHv7A"
pool = redis.ConnectionPool(host='13.211.204.107', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
db = pymysql.connect("13.211.204.107","saicmotor","Saic1234","saicdb")


##根据经纬度查询城市locationkey
def query_location(data=None):
    url="http://dataservice.accuweather.com/locations/v1/cities/geoposition/search"
    #data=event.location
    parms={"apikey":apikey,"q":data}
    r=requests.get(url,parms).json()
    print(r['Key'])
    if "Key" in r:
        return r['Key']
    else:
        return None

## 通过具体的城市查找locationkey
def query_location_bycity(city):
    url = "http://dataservice.accuweather.com/locations/v1/cities/search"
    parms = {"apikey": apikey, "q": city}
    r = requests.get(url, parms).json()
    print(r)
    if "Key" in r[0]:
        print(r[0]["Key"])
        return r[0]["Key"]
    else:
        return None

def  record_rds(city,weatherdata):
    cursor=db.cursor()
    sql = 'insert into cptest(user_id,weather_data,city) VALUES (%s,"%s","%s")' %(111,weatherdata,city)
    print(sql)
    try:
        print("db insert success")
        cursor.execute(sql)
        db.commit()
    except:
        print("db insert error")
        db.rollback()
    #db.close()



def query_weather_byperiod(locationkey,period):
    parms = {"apikey": apikey}
    if "day" in period:
        url="http://dataservice.accuweather.com/forecasts/v1/daily/%s/%s" %(period,locationkey)
    elif "hour" in period:
        url = "http://dataservice.accuweather.com/forecasts/v1/daily/%s/%s" % (period, locationkey)
    else:
        return None
    # r = requests.get(url, parms).json()
    print(url)
    print(r)
    return r



def query_weather_byuser(event,period):
    locationkey=query_location(event["location"])
    query_weather_byperiod(locationkey, period)


def query_weather(event,context):
    if event["method"] == 0:
        locationkey = query_location(event["location"])
        if r.get(locationkey):
            print("redis")
            print(r.get(locationkey))
            return r.get(locationkey)
        else:
            url="http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/%s" %locationkey
            apikey = "mxHDufXzNoqATQJT6xGEoxajC6fhHv7A"
            parms={"apikey":apikey}
            weatherdata=requests.get(url,parms).json()
            r.set(locationkey,weatherdata,24*60*60)
            print(weatherdata)
            return weatherdata
    else:
        city=event["query"]["city"]
        locationkey=query_location_bycity(city)
        if r.get(locationkey):
            print("redis")
            print(r.get(locationkey))
            weatherdata=r.get(locationkey)
            record_rds(city,weatherdata)
            return weatherdata
        else:
            url = "http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/%s" % locationkey
            apikey = "mxHDufXzNoqATQJT6xGEoxajC6fhHv7A"
            parms = {"apikey": apikey}
            weatherdata = requests.get(url, parms).json()
            r.set(locationkey, weatherdata, 24 * 60 * 60)
            record_rds(city, weatherdata)
            print(weatherdata)
            return weatherdata

print("Function End ~~")

if __name__ == '__main__':
    # query_location()
    event={"location":"31,121","query":{"city":"shanghai","period":"3hour"},"method":1}
    query_weather(event,2)
    # record_rds(1,6)