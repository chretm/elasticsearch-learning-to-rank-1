import json
import time
import sys
import os
from utils import Elasticsearch, ES_INDEX, ES_TYPE, ES_DATA, ES_HOST, ES_AUTH, ES_MODEL_NAME, ES_MODEL_TYPE, ES_FEATURE_SET_NAME, ES_METRIC_TYPE
from collectFeatures import logFeatures, buildFeaturesJudgmentsFile
from loadFeatures import initDefaultStore, loadFeatures
from judgments import judgmentsFromFile, judgmentsByQid


class Trainer:

    def __init__(self, es: Elasticsearch):
        self.__es = es

    def train(self):
        # Load features into Elasticsearch
        initDefaultStore()
        loadFeatures(ES_FEATURE_SET_NAME)
        # Parse a judgments
        label_file = self.find_label_file()
        print(self.find_label_file(),file=sys.stderr)
        movieJudgments = judgmentsByQid(
            judgmentsFromFile(filename=label_file))
        # Use proposed Elasticsearch queries (1.json.jinja ... N.json.jinja) to generate a training set
        # output as "sample_judgments_wfeatures.txt"
        logFeatures(self.__es, judgmentsByQid=movieJudgments)
        buildFeaturesJudgmentsFile(
            movieJudgments, filename='sample_judgments_wfeatures.txt')
        # Train each ranklib model type
        results = ""
        model_dct = {0: "MART", 1: "RankNet", 2: "RankBoost",3: "AdaRank", 4:"coord Ascent", 6: "LambdaMART", 7: "ListNET", 8: "Random Forests", 9: "Linear Regression"}
        score = 0
        model = ""
        for modelType in [9,8,6]:
            #modelType = int(ES_MODEL_TYPE)
            print("*** Training %s " % modelType)
            self.trainModel(judgmentsWithFeaturesFile='sample_judgments_wfeatures.txt',
                            modelOutput='model.txt', whichModel=modelType)
            with open('/opt/services/flaskapp/src/training_log.txt') as flog:
                log_lines = flog.readlines()
            
            train_score = float(log_lines[-5].split(":")[1].replace('\n',''))
            val_score = float(log_lines[-4].split(":")[1].replace('\n',''))
            new_score = (1+(val_score-train_score)/train_score)
            #We save the nth model only if the score(validation-training)/training is larger than (n-1)th model
            if new_score > score:
                score = new_score
                model = modelType
                self.saveModel(scriptName=ES_MODEL_NAME,
                           featureSet='movie_features', modelFname='model.txt')
            else:
                pass
            print(label_file)
            results = results + "--------------\n" + '{}{}\n{}'.format('Model trained :' + model_dct[modelType] + '\n', ''.join(log_lines[-5:-3]), str(new_score) + '\n')
        results = results +  'Now test the model: ' + model_dct[model]
        return results

    def trainModel(self, judgmentsWithFeaturesFile, modelOutput, whichModel=6):
        # java -jar RankLib-2.6.jar -ranker 6 -train sample_judgments_wfeatures.txt -save model.txt
        cmd = "java -jar /opt/services/flaskapp/RankLib-2.8.jar -ranker %s -train %s -save %s -metric2t %s -tvs 0.7 -frate 1.0 > /opt/services/flaskapp/src/training_log.txt" % (
            whichModel, judgmentsWithFeaturesFile, modelOutput, ES_METRIC_TYPE)
        print("*********************************************************************")
        print("*********************************************************************")
        print("Running %s" % cmd)
        os.system(cmd)
        pass

    def saveModel(self, scriptName, featureSet, modelFname):
        """ Save the ranklib model in Elasticsearch """
        import requests
        import json
        from urllib.parse import urljoin

        modelPayload = {
            "model": {
                "name": scriptName,
                "model": {
                    "type": "model/ranklib",
                    "definition": {
                    }
                }
            }
        }

        with open(modelFname) as modelFile:
            modelContent = modelFile.read()
            path = "_ltr/_featureset/%s/_createmodel" % featureSet
            fullPath = urljoin(ES_HOST, path)
            modelPayload['model']['model']['definition'] = modelContent
            print("POST %s" % fullPath)
            head = {'Content-Type': 'application/json'}
            resp = requests.post(fullPath, data=json.dumps(
                modelPayload), headers=head, auth=ES_AUTH)
            print(resp.status_code)
            if (resp.status_code >= 300):
                print(resp.text)

    def find_label_file(self):
        return [f for f in os.listdir("/opt/services/flaskapp/src/") if f.endswith("_judgments.txt")][0]
