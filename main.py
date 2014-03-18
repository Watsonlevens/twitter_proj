import sys  # para erros
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

ckey = ''
csecret = ''
atoken = ''
asecret = ''

class listener(StreamListener):

    def on_data(self, data):
        #print data
        try:
            tweet = data.split(',"text":"')[1].split('","source')[0]
            screen_name = data.split(',"screen_name":"')[1].split('","location')[0]
            id_user= data.split('"user":{"id":')[1].split(',"id_str":')[0]
            geo= data.split('},"geo":')[1].split(',"coordinates":')[0]
            coordinates = data.split('"coordinates":')[1].split(',"place":')[0]
            if geo!='null':
                print tweet
                print screen_name
                print id_user
                #print geo
                print coordinates
        except:
            print 'dummy'
        try:
            saveFile = open('twitDB.csv','a')
            encoded_tweet = data.split(',')[3]
            saveFile.write(encoded_tweet)
            saveFile.write('\n')
            saveFile.close()
            
            #print encoded_tweet.decode('unicode-escape')
            return True
        except BaseException, e:
            print 'failed ondata, ', str(e)
        except:
            print "Erro inesperado ", sys.exc_info()[0]
        return True
        
    def on_error(self, status):
        print status
    

def main():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener())
    twitterStream.filter(locations=[-53.109612,-25.250469,-44.160561,-19.779320])
    #SOUTHWEST primeiro, lon/lat
    #sao paulo location=[-53.109612,-25.250469,-44.160561,-19.779320]






main()
