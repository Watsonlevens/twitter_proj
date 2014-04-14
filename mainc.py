# -*- coding: UTF-8 -*-
#decode encode texto vindo do stream e então permitir acentos raw.decode('unicode-escape').encode('utf8')

import sys  # para erros
import os.path # para verificar se o arquivo já existe
import json #facilitar o parser
import re
import time

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from time import gmtime, strftime
import tweepy

ckey = '82WTLmzBPZJX95Cx2DvcWA'
csecret = 'mpQttzhzeR3UlnnUX8qpr2Yd9CxgZNBuf9suQ8TCMFE'
atoken = '97679915-vngv6Jo3w7zseDiYRfajXW74nnr6E4kpSyLKxK5LV'
asecret = 'FuNHIwaMrjc6ixjwJusXqn3GMJc93w06yabzcE7IhmBI9'
ic=['acidente','batida','alagamento',]
ic2= ['ciclismo', 'ciclovia', 'bike', ' bici ', 'pedal']
ic3= ['metro ', 'lotado'] 
ics=[ic,ic2,ic3]
dic0=['[c|C]arro', '[a|A]utom.*vel', '[v|V]e*.culo', '[v|V]iatura', '[c|C]aminh.*o', '[c|C]arreta', '[c|C]aminhonete', '[C|c|k|K]ombi', '[f|F]usca']
dic1=['[o|ô|O|Ô]nibus', '[l|L]ota[ç|c][a|ã]o', '[a|A]rticulado', '[b|B]iarticulado', '[c|C]oletivo', '[b|B]us$']
dic2=['[b|B]icicleta', '[B|b]+i(ke|ci)', '[c|C]iclovia', '[c|C]iclofaixa', '[b|B]iciclet[a|á]rio', '[p|P]araciclo', '[p|P]edalar', '[p|P]edal']
dic3=['[m|M]etr[o|ô]','[t|T]rem']
dic=[dic0,dic1,dic2,dic3]

class listener(StreamListener):

    def on_data(self, data):
        try:
            #print "_________JSON____________"
            jsondata = json.loads(data)
#            print data
           
            if 'text' in jsondata:
                #print jsondata['text'].replace(',','')
                text=strip(jsondata['text'])
            else:
                text=''
            
            coordinate={}
            if 'coordinates' in jsondata and jsondata['coordinates'] is not None:          
                #print jsondata['coordinates']
                coordinate['x']=jsondata['coordinates']['coordinates'][0]
                coordinate['y']=jsondata['coordinates']['coordinates'][1]
                coordinate['type']=jsondata['coordinates']['type']
            else:
                coordinate['x']=''
                coordinate['y']=''
                coordinate['type']=''
            
            if 'created_at' in jsondata and jsondata['created_at'] is not None:
                #print jsondata['created_at'] 
                created_at=jsondata['created_at'] 
            else:
                created_at=''

            if 'entities' in jsondata and jsondata['entities'] is not None:                
                #print jsondata['entities']
                entities='True'
            else:
                entities='False'   

            if 'favorite_count' in jsondata and jsondata['favorite_count'] is not None:
                #print jsondata['favorite_count'] 
                favorited_count=jsondata['favorite_count']
            else:
                favorited_count='0'

            if 'favorited' in jsondata and jsondata['favorited'] is not None:                
                #print jsondata['favorited']
                favorited = jsondata['favorited']
            else:
                favorited = ''
 
            if 'filter_level' in jsondata and jsondata['filter_level'] is not None:
                #print jsondata['filter_level']
                filter_level = jsondata['filter_level']
            else:
                filter_level = ''

            if 'id' in jsondata and jsondata['id'] is not None:                
                #print jsondata['id']
                tweet_id = jsondata['id']
            else:
                tweet_id = ''

            if 'id_str' in jsondata and jsondata['id_str'] is not None:              
                #print jsondata['id_str']
                tweet_id_str = jsondata['id_str']
            else:
                tweet_id_str = ''   

            if 'in_reply_to_screen_name' in jsondata and jsondata['in_reply_to_screen_name'] is not None:
                #print jsondata['in_reply_to_screen_name']
                in_reply_to_name = strip(jsondata['in_reply_to_screen_name'])
                in_reply=True
            else:
                in_reply_to_name = ''
                in_reply=False

            if 'in_reply_to_status_id' in jsondata and jsondata['in_reply_to_status_id'] is not None:
                #print jsondata['in_reply_to_status_id']
                in_reply_to_id = jsondata['in_reply_to_status_id']
            else:
                in_reply_to_id = ''

            if 'in_reply_to_status_id_str' in jsondata and jsondata['in_reply_to_status_id_str'] is not None:
                #print jsondata['in_reply_to_status_id_str']
                in_reply_to_id_str = jsondata['in_reply_to_status_id_str']
            else:
                in_reply_to_id_str = ''                
                
            if 'in_reply_to_user_id' in jsondata and jsondata['in_reply_to_user_id'] is not None:
                #print jsondata['in_reply_to_user_id']
                in_reply_to_user_id = jsondata['in_reply_to_user_id']
            else:
                in_reply_to_user_id = ''  
                
            if 'in_reply_to_user_id_str' in jsondata and jsondata['in_reply_to_user_id_str'] is not None:
                #print jsondata['in_reply_to_user_id_str']
                in_reply_to_user_id_str = jsondata['in_reply_to_user_id_str']
            else:
                in_reply_to_user_id_str = ''                            

            if 'lang' in jsondata and jsondata['lang'] is not None:
                #print jsondata['lang']
                lang=jsondata['lang']
            else:
                lang=''

            if 'place' in jsondata and jsondata['place'] is not None:
                #print jsondata['place']
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
                    #print jsondata['place']['full_name'].replace(',','')
                    place['full_name']=strip(jsondata['place']['full_name'])
                else:
                    place['full_name']=''
                if 'id' in jsondata['place']: 
                    #print jsondata['place']['id']       
                    place['id']=jsondata['place']['id']
                else:
                    place['id']=''
                if 'name' in jsondata['place']:  
                    #print jsondata['place']['name']       
                    place['name']=strip(jsondata['place']['name'])
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
                
            if 'possibly_sensitive' in jsondata and jsondata['possibly_sensitive'] is not None: #pode ser True False ou não ter
                #print jsondata['possibly_sensitive']
                possibly_sensitive=jsondata['possibly_sensitive']
            else:
                possibly_sensitive=''
            
            if 'retweet_count' in jsondata and jsondata['retweet_count'] is not None:
                #print jsondata['retweet_count']
                retweet_count=jsondata['retweet_count']
            else:
                retweet_count=''
                
            if 'retweeted' in jsondata and jsondata['retweeted'] is not None:
                #print jsondata['retweeted']   
                retweeted=jsondata['retweeted']
            else:
                retweeted=''
            
            if 'source' in jsondata and jsondata['source'] is not None:
                #print jsondata['source']
                source=strip(jsondata['source'])
            else:
                source=''
             
            user={}    
            if 'user' in jsondata and jsondata['user'] is not None:
                #print jsondata['user']
                if 'statuses_count' in jsondata['user']:
                    user['statuses_count']=jsondata['user']['statuses_count']
                else:
                    user['statuses_count']=''
                    
                if 'favourites_count' in jsondata['user']:
                    user['favourites_count']=jsondata['user']['favourites_count']
                else:
                    user['favourites_count']=''               

                if 'name' in jsondata['user']:
                    user['name']=strip(jsondata['user']['name'])
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
            
        if 'name' in place and filtro(lang, place['name']):               
            output = text +','+ str(coordinate['x']) +','+ str(coordinate['y']) +','+ coordinate['type'] +','+ created_at +',' + str(created_at.split(' ')[0]) +','+ str(created_at.split(' ')[1]) +',' + str(created_at.split(' ')[2]) +','+ str(created_at.split(' ')[3]) +','+ str(entities) +','+ str(favorited_count) +','+ str(favorited) +','+ filter_level +','+ str(tweet_id) +','+ str(tweet_id_str) +','+ str(in_reply_to_name) +','+ str(in_reply) +','+ str(in_reply_to_id )+','+ in_reply_to_id_str +','+ str(in_reply_to_user_id) +','+ in_reply_to_user_id_str +','+ lang +','+ place['country'] +','+ place['country_code'] +','+ place['full_name'] +','+ str(place['id']) +','+ place['name'] +','+ place['place_type'] +','+ place['url'] +','+ str(possibly_sensitive) +','+ str(retweet_count) +','+ str(retweeted) +','+ source +','+ str(user['statuses_count']) +','+ str(user['favourites_count']) +','+ user['name'] +','+ str(user['verified']) +','+ str(user['followers_count']) +','+ user['screen_name'] +','+ str(user['friends_count']) +','+ user['lang'] +','+ user['created_at']
            write_db('db_tweet.csv',output+termos(text))    
        return True

        
    def on_error(self, status):
        print 'status error',status
        
def write_db(file_name,content):
    try:
        saveFile = open(file_name,'a')
        saveFile.write(content.encode('utf8'))
        saveFile.write('\n')
        saveFile.close()
    except:
        print content
        print 'Problema no content', sys.exc_info()[0]        

def strip(data):
    return data.replace(',',' ').replace("'",' ').replace('"',' ').replace('%',' ').replace('\t', '').replace('\n', '').replace('\r', '').replace('\v', '')
        
def file_exist(file_path):
    return os.path.isfile(file_path)
        
def cabecalho(file_name):
    try:
        saveFile = open(file_name,'a')
        cabecalho = "text,coordinate_x,coordinate_y,coordinat_type,tweet_created_at,dia_semana,mes,dia,hora,tweet_entities,tweet_favorited_count,tweet_favorited,filter_level,tweet_id,tweet_id_str,in_reply_to_name,in_reply,in_reply_to_id,in_reply_to_id_str,in_reply_to_user_id,in_reply_to_user_id_str,tweet_lang,tweet_country,tweet_country_code,tweet_place_full_name,tweet_place_id,tweet_place_name,tweet_place_type,tweet_place_url,possibly_sensitive,retweet_count,retweeted,source,user_statuses_count,user_favourites_count,user_name,user_verified,user_followers_count,user_screen_name,user_friends_count,user_lang,user_created_at"
        cabecalho+="" 
        cabecalho+=",carro,automovel,veiculo,viatura,caminhao,carreta,caminhonete,kombi,fusca,onibus,lotacao,articulado,biarticulado,coletivo,bus,bicicleta,bici_bike,ciclovia,ciclofaixa,bicicletario,paraciclo,pedalar,pedal,metro,trem"
        saveFile.write(cabecalho)
        saveFile.write('\n')
        saveFile.close()
    except:
        print 'Problema no cabeçalho ', sys.exc_info()[0]

def cabecalho2(file_name):
    try:
        saveFile = open(file_name,'a')
        cabecalho = "text,coordinate_x,coordinate_y,coordinate_type,tweet_created_at,dia_semana,mes,dia,hora,tweet_entities,tweet_favorited_count,tweet_favorited,filter_level,tweet_id,tweet_id_str,in_reply_to_name,in_reply,in_reply_to_id,in_reply_to_id_str,in_reply_to_user_id,in_reply_to_user_id_str,tweet_lang,tweet_country,tweet_country_code,tweet_place_full_name,tweet_place_id,tweet_place_name,tweet_place_type,tweet_place_url,retweet_count,retweeted,source,user_statuses_count,user_favourites_count,user_name,user_verified,user_followers_count,user_screen_name,user_friends_count,user_lang,user_created_at"
        cabecalho+="" 
        cabecalho+=",carro,automovel,veiculo,viatura,caminhao,carreta,caminhonete,kombi,fusca,onibus,lotacao,articulado,biarticulado,coletivo,bus,bicicleta,bici_bike,ciclovia,ciclofaixa,bicicletario,paraciclo,pedalar,pedal,metro,trem"
        saveFile.write(cabecalho)
        saveFile.write('\n')
        saveFile.close()
    except:
        print 'Problema no cabeçalho ', sys.exc_info()[0]

def filtro(lang, place_name):
    lang_ban=['und']
    cidades=['Brasil','Brásil','São Paulo', 'Sao Paulo', 'Brazil']
    
    if lang in lang_ban: 
        return False
    else:
        if place_name.encode('utf8') in cidades: 
            print ('salvando '+place_name)
            return True
        else:
            print (place_name)
            return False

def termos(tweet):
    output=""
    for d in dic:
        for termo in d:
            if re.search(termo,tweet):
                print termo
                output+=",1"
            else:
                output+=",0"           
    return output


def search(api, termo, dbt):
    #-23.547778, -46.635833 50 km de raio
    i=0
    for tweet in tweepy.Cursor(api.search,
                           q=termo,
                           count=100,
                           geocode="-23.547778,-46.635833,50km",
                           result_type="recent",
                           include_entities=True,
                           lang="pt").items():
#        i+=1
#        if i%1000==0:
#            time.sleep(11)
#            i=0
        text=''
        coordinate={}
        coordinate['x']=''
        coordinate['y']=''
        coordinate['type']=''
        created_at=''
        entities='False'
        favorited_count='0'
        favorited=''
        filter_level=''
        tweet_id=''
        tweet_id_str=''
        in_reply_to_name=''
        in_reply='False'
        in_reply_to_id=''
        in_reply_to_id_str=''
        in_reply_to_user_id=''
        in_reply_to_user_id_str=''
        lang=''
        place={}
        place['country']=''
        place['country_code']=''
        place['full_name']=''
        place['id']=''
        place['name']=''
        place['place_type']=''
        place['url']=''
        retweet_count=''
        retweeted=''
        source=''
        user={}
        user['statuses_count']=''
        user['favourites_count']=''
        user['name']=''
        user['verified']=''
        user['followers_count']=''
        user['screen_name']=''
        user['friends_count']=''
        user['lang']=''
        user['created_at']=''
        metadata={}
        metadata['result_type']=''
        metadata['iso_language_code']='' 
               
        #print '\n'
        #print tweet
        if tweet.place is not None:
            #print tweet.place
            place['country']=tweet.place.country
            place['country_code']=tweet.place.country_code
            place['full_name']=strip(tweet.place.full_name)
            place['id']=tweet.place.id
            place['name']=strip(tweet.place.name)
            place['place_type']=tweet.place.place_type
            place['url']=strip(tweet.place.url)
        if tweet.user is not None:#mesma coisa que author
            #print tweet.user
            user['statuses_count']=tweet.user.statuses_count
            user['favourites_count']=tweet.user.favourites_count
            user['name']=strip(tweet.user.name)
            user['verified']=tweet.user.verified
            user['followers_count']=tweet.user.followers_count
            user['screen_name']=strip(tweet.user.screen_name)
            user['friends_count']=tweet.user.friends_count
            user['lang']=tweet.user.lang
            user['created_at']=tweet.user.created_at
            #print user
        if tweet.coordinates is not None:
            #print tweet.coordinates
            coordinate['x']=tweet.coordinates['coordinates'][0]
            coordinate['y']=tweet.coordinates['coordinates'][1]
            coordinate['type']=tweet.coordinates['type']
            #print coordinate
        if tweet.created_at is not None:
            #print tweet.created_at.weekday()
            #print tweet.created_at.month
            #print tweet.created_at.day
            #print tweet.created_at.hour
            created_at=tweet.created_at
            #print created_at.date(),created_at.time()
            #print dir(created_at)
        if tweet.entities is not None:
            #print tweet.entities
            entities='True'
        if tweet.favorite_count is not None:
            #print tweet.favorite_count
            favorited_count=tweet.favorite_count
        if tweet.favorited is not None:
            #print tweet.favorited
            favorited=tweet.favorited
        if tweet.id is not None:
            #print tweet.id
            tweet_id=tweet.id
        if tweet.id_str is not None:
            #print tweet.id_str
            tweet_id_str=tweet.id_str
        if tweet.in_reply_to_screen_name is not None:
            #print tweet.in_reply_to_screen_name
            in_reply_to_name=strip(tweet.in_reply_to_screen_name)
            in_reply='True' 
        if tweet.in_reply_to_status_id is not None:
            #print tweet.in_reply_to_status_id
            in_reply_to_id=tweet.in_reply_to_status_id
        if tweet.in_reply_to_status_id_str is not None:
            #print tweet.in_reply_to_status_id_str
            in_reply_to_id_str=tweet.in_reply_to_status_id_str
        if tweet.in_reply_to_user_id        is not None:
            #print tweet.in_reply_to_user_id
            in_reply_to_user_id=tweet.in_reply_to_user_id 
        if tweet.in_reply_to_user_id_str is not None:
            #print tweet.in_reply_to_user_id_str 
            in_reply_to_user_id_str=tweet.in_reply_to_user_id_str         
    
        if tweet.lang is not None:
            #print tweet.lang  
            lang=tweet.lang
        if tweet.metadata is not None:
            #print tweet.metadata['result_type']
            #print tweet.metadata['iso_language_code']
            metadata['result_type']=tweet.metadata['result_type']
            metadata['result_type']=tweet.metadata['iso_language_code']
        if tweet.place is not None:
            #print tweet.place
            place['country']=tweet.place.country
            place['country_code']=tweet.place.country_code
            place['full_name']=strip(tweet.place.full_name)
            place['id']=tweet.place.id
            place['name']=strip(tweet.place.name)
            place['place_type']=tweet.place.place_type
            place['url']=tweet.place.url
            #print place 
        if tweet.retweet_count is not None:
            #print tweet.retweet_count
            retweet_count=tweet.retweet_count
        if tweet.retweeted is not None:
            #print tweet.retweeted
            retweeted=tweet.retweeted
        if tweet.source is not None:
            #print tweet.source
            source=tweet.source
        if tweet.text is not None:
            #print tweet.text
            text=strip(tweet.text)
        if place['name']!='' and filtro(lang, place['name']): 
         
            output = ','.join([text, str(coordinate['x']), str(coordinate['y']), coordinate['type'], str(created_at), str(created_at.weekday()), str(created_at.month), str(created_at.day), str(created_at.hour), str(entities), str(favorited_count), str(favorited), filter_level, str(tweet_id), str(tweet_id_str), str(in_reply_to_name), str(in_reply), str(in_reply_to_id ), in_reply_to_id_str, str(in_reply_to_user_id), in_reply_to_user_id_str, lang, place['country'], place['country_code'], place['full_name'], str(place['id']), place['name'], place['place_type'], place['url'], str(retweet_count), str(retweeted), source, str(user['statuses_count']), str(user['favourites_count']), user['name'], str(user['verified']), str(user['followers_count']), user['screen_name'], str(user['friends_count']), user['lang'], str(user['created_at'])])
            write_db(dbt,output+termos(text))           
        

def get_db(api):

    carro=['carro','automovel','veiculo','viatura','caminhao','carreta','caminhonete','combi','kombi',]
    onibus=['onibus','lotacao','articulado','coletivo','bus']
    bicicleta=['bici','bike','bicicleta','ciclovia','ciclofaixa','bicicletario', 'paraciclo', 'pedalar', 'pedal']
    trem=['trem','metro']
    
    #db_tweetauto.csv db_tweetonibus.csv db_tweetbicicleta.csv db_tweettrem.csv 
#    if not file_exist('db_tweetauto.csv'):
#        cabecalho2('db_tweetauto.csv')  
#    for termo in carro:
#        search(api,termo,'db_tweetauto.csv')
        
#    if not file_exist('db_tweetonibus.csv'):
#        cabecalho2('db_tweetonibus.csv')  
#    for termo in onibus:
#        search(api,termo,'db_tweetonibus.csv')
        
#    if not file_exist('db_tweetbicicleta.csv'):
#        cabecalho2('db_tweetbicicleta.csv')  
#    for termo in bicicleta:
#        search(api,termo,'db_tweetbicicleta.csv')

    if not file_exist('db_tweettrem.csv'):
        cabecalho2('db_tweettrem.csv')  
    for termo in trem:
        search(api,termo,'db_tweettrem.csv')
    
def main():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    api = tweepy.API(auth)
    print "Testes search"    
    get_db(api)

main()
