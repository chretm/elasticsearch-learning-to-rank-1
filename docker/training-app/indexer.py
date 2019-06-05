import json
import time
import sys
from utils import Elasticsearch, ES_INDEX, ES_TYPE, ES_DATA

class Indexer:

    def __init__(self, es: Elasticsearch):
        self.__es = es

    def prepare(self):
        try:
            print('Elasticsearch is alive',file=sys.stderr)
            with open(ES_DATA) as data:
                movieDict = json.loads(data.read())
                self.__reindex(self.__es, movieDict=movieDict, index=ES_INDEX, es_type=ES_TYPE)
        except:
            print('Upps ... wating for elasticsearch')
            time.sleep(5)
            self.prepare()  

        return "Index prepared, now prepare labels"

    def __enrich(self, movie):
        """ Enrich for search purposes """
        if 'title' in movie:
            movie['title_sent'] = 'SENTINEL_BEGIN ' + movie['title']

    def __bulkDocs(self, movieDict, index, es_type):
            #chretm 2019-06-05 : we define the list of items we want to use for the index
            to_keep = ['genres','original_language','title','overview','popularity', 'production_companies']
            to_keep.extend(['production_countries', 'release_date', 'revenue','spoken_languages', 'status'])
            to_keep.extend(['vote_average','vote_count'])
            dictfilt = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])  #This function takes x as dict and keeps keys in y
            for id, movie in movieDict.items():
                movie = dictfilt(movie, to_keep)
                if 'release_date' in movie and movie['release_date'] == "":
                    del movie['release_date']
                #chretm : we 
                self.__enrich(movie)
                addCmd = {"_index": index,
                          "_type": es_type,
                          "_id": id,
                          "_source": movie}
                yield addCmd
                if 'title' in movie:
                    print("%s added to %s" % (movie['title'], index),file=sys.stderr)

    def __reindex(self, es, analysisSettings={}, mappingSettings={}, movieDict={}, index='tmdb', es_type='movie'):
        import elasticsearch.helpers
        settings = {
            "settings": {
                "number_of_shards": 1,
                "index": {
                    "analysis" : analysisSettings,
                }}}

        if mappingSettings:
            settings['mappings'] = mappingSettings

        es.indices.delete(index, ignore=[400, 404])
        es.indices.create(index, body=settings)
        elasticsearch.helpers.bulk(es, self.__bulkDocs(movieDict, index, es_type))