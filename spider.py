#!/usr/bin/env python
# coding: utf-8

from pprint import pprint
import re
import pickle

import httplib2 # dica do Mark Pilgrimm
from tornado import httpclient, ioloop, gen

URL_CATALOGO = 'http://novatec.com.br/catalogo.php'
URL_PRODUTOS = 'http://novatec.com.br/livros/'

# RE_SLUG_LIVRO = re.compile(r'href="livros/(.+?)"', re.DOTALL)
RE_SLUG_LIVRO = re.compile(r'href="livros/([^"/]+)', re.DOTALL)

'''<a class="style12" href="javascript:AmpliarCapa(
   'livros/uml2abordagempratica/capa_ampliada9788575221938.jpg')
   ;">[ + ] capa ampliada</a>'''

RE_CAPA = re.compile(r"AmpliarCapa\('livros/([^']+)")

def buscar_slugs(url):
    try:
        with open('slugs.pickle') as slugs_pickle:
            slugs = pickle.load(slugs_pickle)
    except (IOError, EOFError):
        h = httplib2.Http('.cache')
        resp, content = h.request(url)
        #pprint(resp)
        #print len(content), 'bytes lidos'
        slugs = RE_SLUG_LIVRO.findall(content)
        slugs = sorted(set(slugs))
        with open('slugs.pickle', 'wb') as slugs_pickle:
            pickle.dump(slugs, slugs_pickle, -1)        
    return slugs
    
def buscar_produtos(slugs):
    cliente_http = httpclient.AsyncHTTPClient()
    pendentes = set(slugs)
    for slug in slugs:
        baixar(slug, cliente_http, pendentes)
        
@gen.engine
def baixar(slug, cliente, pendentes):
    resp = yield gen.Task(cliente.fetch, URL_PRODUTOS+slug)
    if resp.error:
        print '*** Erro ao baixar', slug
        print '\t', resp.error
    else:
        print 'baixado: ', slug
        path_capa = RE_CAPA.search(resp.body)
        if path_capa is None:
            print '*** Path capa nao encontrado em:', slug
        else:
            path_capa = path_capa.group(1)
            resp_capa = yield gen.Task(cliente.fetch, URL_PRODUTOS+path_capa)
            if resp_capa.error:
                print '*** Erro ao baixar', path_capa
                print '\t', resp_capa.error
            else:
                nome_img = path_capa.replace('/','_')
                with open(nome_img, 'wb') as img:
                    img.write(resp_capa.body)
                print 'salvo: ', nome_img
            
    pendentes.remove(slug)
    
    
    
    if not pendentes:
        ioloop.IOLoop.instance().stop()
        
if __name__=='__main__':
    slugs = buscar_slugs(URL_CATALOGO)
    #pprint(slugs)
    # slugs = slugs[:5] # para testar mais rapido
    buscar_produtos(slugs)
    ioloop.IOLoop.instance().start()
