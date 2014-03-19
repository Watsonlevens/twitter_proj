# -*- coding: UTF-8 -*-

import sys  # para erros
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from time import gmtime, strftime

ckey = ''
csecret = ''
atoken = ''
asecret = ''
dic=['acidente','batida','alagamento',]
dic2= ['ciclismo', 'ciclovia', 'bike', ' bici ', 'pedal']
dic3= ['metro ', 'lotado'] 
dics=[dic,dic2,dic3]

class listener(StreamListener):

    def on_data(self, data):
        try:
            
            tweet = data.split(',"text":"')[1].split('","source')[0]
            screen_name = data.split(',"screen_name":"')[1].split('","location')[0]
            id_user= data.split('"user":{"id":')[1].split(',"id_str":')[0]
            geo= data.split('},"geo":')[1].split(',"coordinates":')[0]
            coordinates = data.split('"coordinates":')[1].split(',"place":')[0]
            coordinatex = coordinates[1:coordinates.find(',')]
            coordinatey = coordinates[coordinates.find(',')+1:len(coordinates)-3]
            place=data.split('"place_type":"')[1].split('","name":')[0]
            if place=='city':
                city=data.split('city","name":"')[1].split('","full_name"')[0]
            else:
                city='null'
            if geo!='null':
                for d in dics:
                    for palavra in d:
                        if tweet.find(palavra)!=-1:
                            print tweet.decode('unicode-escape')
                            print screen_name.decode('unicode-escape')
                            print id_user
                            #print geo
                            print coordinates
                            print place.decode('unicode-escape')
                            print city.decode('unicode-escape')
                            print coordinatex
                            print coordinatey
                            
                            saveFile = open('twitDBfiltered.csv','a')
                            encoded_tweet = strftime("%H:%M:%S")+','+'"'+ tweet+'"' +','+ screen_name+',' + id_user+',' + coordinatex+','+coordinatey+','+city
                            saveFile.write(encoded_tweet)
                            saveFile.write('\n')
                            saveFile.close()
        except:
            print 'dummy'
        try:
            if geo!='null':
                saveFile = open('twitDB.csv','a')
                encoded_tweet = strftime("%H:%M:%S")+','+'"'+ tweet+'"' +','+ screen_name+',' + id_user+',' + coordinatex+','+coordinatey+','+city
                saveFile.write(encoded_tweet)
                saveFile.write('\n')
                saveFile.close()
                #if city==('S\u00e3o Paulo'):
                #print encoded_tweet.decode('unicode-escape')
            return True
        except BaseException, e:
            print 'failed ondata, ', str(e)
        except:
            print "Erro inesperado ", sys.exc_info()[0]
        return True
        
    def on_error(self, status):
        print 'status error',status
    

def main():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener())
    twitterStream.filter(locations=[-46.825390,-24.008381,-46.364830,-23.357611])
    #SOUTHWEST primeiro, lon/lat
    #estado de sao paulo location=[-53.109612,-25.250469,-44.160561,-19.779320]
    #cidade de sao paulo -46.825390,-24.008381,-46.364830,-23.357611
    






main()
