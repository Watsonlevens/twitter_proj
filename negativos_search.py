# -*- coding: UTF-8 -*-
import csv
import re
import os.path
# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

__author__="DaviR"
__date__ ="$14/04/2014 14:18:36$"


def contatermos(row):
    termos = sum(map(int, row[41:]))
    #print termos
    return termos
    
def lerarquivo(arq,tipo):
    reader= csv.reader(open(arq+'.csv', 'rU'))
    writer = csv.writer(open(arq+'_new.csv','w'),lineterminator='\n')
    header=next(reader)
    header.append('termos')
    header.append('tipo')
    writer.writerow(header)
    vet=[]
    rep=0
    for row in reader:
        if row[14] not in vet:
            vet.append(row[14])
            row.append(contatermos(row))
            row.append(tipo)
            writer.writerow(row)
        else:
            #print 'repetido'
            rep+=1
    print "REMOVIDOS "+str(rep)
    return True

def write_db(file_name,content):
    try:
        saveFile = open(file_name,'a')
        saveFile.write(content)
        saveFile.write('\n')
        saveFile.close()
    except:
        print content
        print 'Problema no content', sys.exc_info()[0]  

def termos(tweet,dic):
    output=""
    for termo in dic:
        if re.search(termo,tweet):
            print termo
            output+=",1"
        else:
            output+=",0"           
    return output

def cabecalho(text,dic):
    return text[:-1]+','+','.join(dic)

    
def termosnegativos(arq,tipo):
    ttrem=['lotado','cheio','greve','paralisa��o','sem opera��o','n�o operante','problema linha', 'demora']
    tauto=['tr�nsito lento','obras na faixa','sem acesso','demora','rota inacess�vel','faixa bloqueada','problema via',
            'desvio','sem sinaliza��o','n�o sinalizado','pista interrompida','atropelamento','buraco']
    tonibus=['n�o passou','lotado','cheio','tr�nsito lento','obras na faixa','defeito elevador','sem acesso','demora',
            'rota inacess�vel','faixa bloqueada','problema via','desvio','sem sinaliza��o','greve','paralisa��o','atropelamento',
            'buraco']
    tbicicleta=['sem ciclovia','sem ciclofaixa','n�o tem','estragado','quebrado']
    dtipos={'T' : ttrem, 'C' : tauto, 'O' : tonibus, 'B' : tbicicleta}
    
    retrem=['[l|L]otado','[c|C]heio','[g|G]reve','[p|P]arali[s|�|z]a[c|�|s][a|�]o','[s|S]em.+opera[c|�|s][a|�]o',
            '[n|N][a|�]o.+operante','[p|P]roblema.+linha','[D|d]emora']
    reauto=['[t|T]r[�|a]nsito.+lento','[o|O]bra.+faixa','[s|S]em.+acesso','demora','[r|R]ota.+inacess[i|�]vel','[f|F]aixa.+bloqueada',
            '[p|P]roblema.+via','[d|D]esvio','[s|S]em.+sinali[z|s]a[�|c|s][a|�]o','[N|n][a|�]o.+sinali[z|s]ado',
            '[p|P]ista.+inte[rr|r]ompida','[a|A]tropela', '[b|B]uraco']
    reonibus=['[n|N][a|�]o.+pa[ss|�|c]ou','[l|L]otado','[c|C]heio','[t|T]r[a|�]nsito.+lento','[o|O]bra.+faixa',
            '([d|D]efeito|[p|P]roblema).+elevado','[s|S]em.+acesso','[D|d]emora','[r|R]ota.+inacess[i|�]vel',
            '[f|F]aixa.+bloquead[o|a]','[p|P]roblema.+via','[d|D]esvio','[s|S]em.+sinali[z|s]a[�|c|s][a|�]o',
            '[g|G]reve','[P|p]arali[s|z]a[�|c|s][a|�]o','[a|A]tropela','[b|B]uraco']
    rebicicleta=['[s|S]em.+ciclovia','[s|S]em.+ciclofaixa','[n|N][a|�]o.+tem' ,'[e|E]stragado','[q|Q]uebrado']
    retipos={'T' : retrem, 'C' : reauto, 'O' : reonibus, 'B' : rebicicleta}
    
    print dtipos[tipo]
    
    input = open(arq, 'r')
    
    #criar cabecalho
    if not os.path.isfile('new'+arq):
        write_db('neg_'+arq, cabecalho(input.next(),dtipos[tipo])) 
    
    #analisar tweets em busca de termos
    for row in input:
        write_db('neg_'+arq, row[:-1]+termos(row.split(',')[0], retipos[tipo]))
    print 'FINISH'
    return

def removerepetidos_contatermos():
    print "Removendo duplicados..."
    lerarquivo('db_tweettrem','T')
    lerarquivo('db_tweetauto','C')
    lerarquivo('db_tweetbicicleta','B')
    lerarquivo('db_tweetonibus','O')
    return

def loadtermosnegativos():
    try:
        print 'Localizando termos negativos...'
        termosnegativos('db_tweettrem'+'_new.csv', 'T') 
        termosnegativos('db_tweetauto'+'_new.csv', 'C')
        termosnegativos('db_tweetonibus'+'_new.csv', 'O')
        termosnegativos('db_tweetbicicleta'+'_new.csv', 'B')
    except:
        print'algo errado com os termos negativos'
    return

def agregadordearquivos():
    ttrem=['t_lotado','t_cheio','t_greve','t_paralisa��o','t_sem opera��o','t_n�o operante','t_problema linha', 't_demora']
    tauto=['c_tr�nsito lento','c_obras na faixa','c_sem acesso','c_demora','c_rota inacess�vel','c_faixa bloqueada','c_problema via',
            'c_desvio','c_sem sinaliza��o','c_n�o sinalizado','c_pista interrompida','c_atropelamento','c_buraco']
    tonibus=['o_n�o passou','o_lotado','o_cheio','o_tr�nsito lento','o_obras na faixa','o_defeito elevador','o_sem acesso','o_demora',
            'o_rota inacess�vel','o_faixa bloqueada','o_problema via','o_desvio','o_sem sinaliza��o','o_greve','o_paralisa��o','o_atropelamento',
            'o_buraco']
    tbicicleta=['b_sem ciclovia','b_sem ciclofaixa','b_n�o tem','b_estragado','b_quebrado']
    dtipos={'T' : ttrem, 'C' : tauto, 'O' : tonibus, 'B' : tbicicleta}
    
    ftrem=open('neg_db_tweettrem_new.csv','r')
    fauto=open('neg_db_tweetauto_new.csv','r')
    fonibus=open('neg_db_tweetonibus_new.csv', 'r')
    fbike=open('neg_db_tweetbicicleta_new.csv','r')
    
    #fagreg=open('output.csv', 'a')
    cabecalho = ','.join(ftrem.next().split(',')[0:68]+ttrem+tauto+tonibus+tbicicleta)
    write_db('output.csv', cabecalho)
    print cabecalho
    #print cabecalho.count(',')
    
    fauto.next()
    fonibus.next()
    fbike.next()
    
    tam_t=len(ttrem)
    tam_a=len(tauto)
    tam_o=len(tonibus)
    tam_b=len(tbicicleta)
    
    #print ','.join(ftrem.next()[:-1].split(',')[68:]+['0']*(tam_a+tam_o+tam_b))
    #print ','.join(['0']*tam_t+fauto.next()[:-1].split(',')[68:]+['0']*(tam_o+tam_b))
    #print ','.join((['0']*(tam_t+tam_a))+fonibus.next()[:-1].split(',')[68:]+['0']*(tam_b))
    #print ','.join((['0']*(tam_t+tam_a+tam_o))+fbike.next()[:-1].split(',')[68:])
    
    print "Juntando arquivos..."
    print "Trem"
    for linha in ftrem:
        content=','.join(linha.split(',')[0:68])+','
        um_zero=','.join(linha[:-1].split(',')[68:]+['0']*(tam_a+tam_o+tam_b))
        write_db('output.csv', content+um_zero)
    
    print "Auto"
    for linha in fauto:
        content=','.join(linha.split(',')[0:68])+','
        um_zero=','.join(['0']*tam_t+linha[:-1].split(',')[68:]+['0']*(tam_o+tam_b))
        write_db('output.csv', content+um_zero)
    
    print "Onibus"
    for linha in fonibus:
        content=','.join(linha.split(',')[0:68])+','
        um_zero=','.join((['0']*(tam_t+tam_a))+linha[:-1].split(',')[68:]+['0']*(tam_b))
        write_db('output.csv', content+um_zero)
    
    print "Bike"
    for linha in fbike:
        content=','.join(linha.split(',')[0:68])+','
        um_zero=','.join((['0']*(tam_t+tam_a+tam_o))+linha[:-1].split(',')[68:])
        write_db('output.csv', content+um_zero)
    return

if __name__ == "__main__":
    # Trem T Auto C Onibus O Bicicleta B
    #removerepetidos_contatermos()
    #loadtermosnegativos()
    agregadordearquivos()


