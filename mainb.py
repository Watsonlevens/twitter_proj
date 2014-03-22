# -*- coding: UTF-8 -*-
#decode encode texto vindo do stream e então permitir acentos raw.decode('unicode-escape').encode('utf8')

import sys  # para erros
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from time import gmtime, strftime

ckey = '82WTLmzBPZJX95Cx2DvcWA'
csecret = 'mpQttzhzeR3UlnnUX8qpr2Yd9CxgZNBuf9suQ8TCMFE'
atoken = '97679915-vngv6Jo3w7zseDiYRfajXW74nnr6E4kpSyLKxK5LV'
asecret = 'FuNHIwaMrjc6ixjwJusXqn3GMJc93w06yabzcE7IhmBI9'
dic=['acidente','batida','alagamento',]
dic2= ['ciclismo', 'ciclovia', 'bike', ' bici ', 'pedal']
dic3= ['metro ', 'lotado'] 
dics=[dic,dic2,dic3]

class listener(StreamListener):

    def on_data(self, data):
        try:
            print "_________JSON____________"
            jsondata = json.loads(data)
            #print data
           
            if hasattr(jsondata, 'text'):
                #print jsondata['text']
                text=jsondata['text'] 
            else:
                text=''
            
            if hasattr(jsondata, 'coordinates'):  # pode retornar none caso nao tenha sido definido
                #print jsondata['coordinates']
                if jsondata['coordinates']['coordinates'][0]:
                    coordinate_x=jsondata['coordinates']['coordinates'][0]
                else:
                    coordinate_x=''
                if jsondata['coordinates']['coordinates'][1]:
                    coordinate_y=jsondata['coordinates']['coordinates'][1]
                else:
                    coordinate_y=''
                if jsondata['coordinates']['type']:
                    coordinate_type=jsondata['coordinates']['type']
                else:
                    coordinate_type=''

            if hasattr(jsondata, 'created_at'):
#                print jsondata['created_at'] 
                created_at=jsondata['created_at'] 
            else:
                created_at=''

            if hasattr(jsondata, 'entities'):                
                #print jsondata['entities']
                entities='True'
            else:
                entities='False'   

            if hasattr(jsondata, 'favorite_count'):                 
                #print jsondata['favorite_count'] 
                favorited_count=jsondata['favorite_count']
            else:
                favorited_count='0'

            if hasattr(jsondata, 'favorited'):                
                #print jsondata['favorited']
                favorited = True
            else:
                favorited = False
 
            if hasattr(jsondata, 'filter_level'):               
                #print jsondata['filter_level']
                filter_level = jsondata['filter_level']
            else:
                filter_level = ''

            if hasattr(jsondata, 'id'):                
                #print jsondata['id']
                tweet_id = jsondata['id']
            else:
                tweet_id = ''

            if hasattr(jsondata, 'id_str'):            
                #print jsondata['id_str']
                tweet_id_str = jsondata['id_str']
            else:
                tweet_id_str = ''   

            if hasattr(jsondata, 'in_reply_to_screen_name'):                
                #print jsondata['in_reply_to_screen_name']
                in_reply_to_name = jsondata['in_reply_to_screen_name']
                in_reply=True
            else:
                in_reply_to = ''
                in_reply=False

            if hasattr(jsondata, 'in_reply_to_status_id'):            
                #print jsondata['in_reply_to_status_id']
                in_reply_to_id = jsondata['in_reply_to_status_id']
            else:
                in_reply_to_id = ''

            if hasattr(jsondata, 'n_reply_to_status_id_str'):                  
                #print jsondata['in_reply_to_status_id_str']
                in_reply_to_id = jsondata['in_reply_to_status_id_str']
            else:
                in_reply_to_id = ''                
                
            if hasattr(jsondata, 'in_reply_to_user_id'):  
                #print jsondata['in_reply_to_user_id']
                in_reply_to_user_id = jsondata['in_reply_to_user_id']
            else:
                in_reply_to_user_id = ''  
                
            if hasattr(jsondata, 'in_reply_to_user_id_str'):  
                #print jsondata['in_reply_to_user_id']
                in_reply_to_user_id_str = jsondata['in_reply_to_user_id_str']
            else:
                in_reply_to_user_id_str = ''                            

            if hasattr(jsondata, 'lang'):              
                #print jsondata['lang']
                lang=jsondata['lang']
            else:
                lang=''

            if hasattr(jsondata, 'place'):  
                place={}
                if 'country' in jsondata['place']:
                    #print jsondata['place']['country']         
                    place['country']=jsondata['place']['country']
                else:
                    place['country']=''
                if 'country_code' in jsondata['place']: 
                    #print jsondata['place']['country_code']        
                    place['country_code']=jsondata['place']['country_code']
                else:
                    place['country_code']=''
                if 'full_name' in jsondata['place'] : 
                    #print jsondata['place']['full_name']
                    place['full_name']=jsondata['place']['full_name']
                else:
                    place['full_name']=''
                if 'id' in jsondata['place']: 
                    #print jsondata['place']['id']       
                    place['id']=jsondata['place']['id']
                else:
                    place['id']=''
                if 'name' in jsondata['place']:  
                    #print jsondata['place']['name']       
                    place['name']=jsondata['place']['name']
                else:
                    place['name']=''
                if 'place_type' in jsondata['place']:   
                    #print jsondata['place']['place_type']     
                    place['place_type']=jsondata['place']['place_type']
                else:
                    place['place_type']=''
                if 'url' in jsondata['place']:
                    #print jsondata['place']['url']      
                    place['url']=jsondata['place']['url']
                else:
                    place['url']=''
                
            if hasattr(jsondata, 'possibly_sensitive'): #pode ser True False ou não ter
                #print jsondata['possibly_sensitive']
                possibly_sensitive=jsondata['possibly_sensitive']
            else:
                possibly_sensitive=''
            
            if hasattr(jsondata, 'possibly_sensitive'): 
                print jsondata['retweet_count']
                retweet_count=jsondata['retweet_count']
            else:
                retweet_count=''
                
            if hasattr(jsondata, 'retweeted'): 
                #print jsondata['retweeted']   
                retweeted=jsondata['retweeted']
            else:
                retweeted=''
            
            if hasattr(jsondata, 'source'):
                #print jsondata['source']
                source=jsondata['source']
            else:
                source=''
                
            if hasattr(jsondata, 'user'):
                user={}              

                if 'statuses_count' in jsondata['user']:
                    user['statuses_count']=jsondata['user']['statuses_count']
                else:
                    user['statuses_count']=''
                    
                if 'favourites_count' in jsondata['user']:
                    user['favourites_count']=jsondata['user']['favourites_count']
                else:
                    user['favourites_count']=''               

                if 'name' in jsondata['user']:
                    user['name']=jsondata['user']['name']
                else:
                    user['name']='' 

                if 'verified' in jsondata['user']:
                    user['verified']=jsondata['user']['verified']
                else:
                    user['verified']=''
                    
                if 'followers_count' in jsondata['user']:
                    user['followers_count']=jsondata['user']['followers_count']
                else:
                    user['followers_count']=''                    
            
                if 'screen_name' in jsondata['user']:
                    user['screen_name']=jsondata['user']['screen_name']
                else:
                    user['screen_name']=''  
                    
                if 'friends_count' in jsondata['user']:
                    user['friends_count']=jsondata['user']['friends_count']
                else:
                    user['friends_count']=''                    
                            
                if 'lang' in jsondata['user']:
                    user['lang']=jsondata['user']['lang']
                else:
                    user['lang']=''

                if 'created_at' in jsondata['user']:
                    user['created_at']=jsondata['user']['created_at']
                else:
                    user['created_at']=''                                                 
           
        except:
            print 'dummy', sys.exc_info()[0]
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
