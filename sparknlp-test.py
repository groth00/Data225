#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 22:51:59 2021
@author: groth
"""

import pandas as pd
import csv
import sparknlp
from pyspark.ml import Pipeline
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline, PipelineModel

def createPipeline():
    documentAssembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")
    sentenceDetector = SentenceDetector() \
        .setInputCols(["document"]) \
        .setOutputCol("sentence")
    tokenizer = Tokenizer() \
        .setInputCols(["sentence"]) \
        .setOutputCol("token")
    wordEmbeddings = WordEmbeddingsModel.pretrained("glove_6B_300", "xx")\
        .setInputCols(["document", "token"])\
            .setOutputCol("embeddings")
    nerModel = NerDLModel.pretrained("ner_aspect_based_sentiment")\
        .setInputCols(["document", "token", "embeddings"])\
        .setOutputCol("ner")
    nerConverter = NerConverter()\
        .setInputCols(["sentence", "token", "ner"])\
        .setOutputCol("ner_chunk")
    nlp_pipeline = Pipeline(stages=[documentAssembler, sentenceDetector, \
        tokenizer, wordEmbeddings, nerModel, nerConverter])
    return nlp_pipeline

def initializeModel(nlp_pipeline):
    empty_df = spark.createDataFrame([['']]).toDF("text")
    pipeline_model = nlp_pipeline.fit(empty_df)
    lightPipeline = LightPipeline(pipeline_model)
    return lightPipeline
    
def getReviews(business_id):
    return reviews_MA[reviews_MA.business_id.isin([business_id])]\
        .text.tolist()
    
def annotateReviews(text_input, lightPipeline):
    results = lightPipeline.fullAnnotate(text_input)
    return results
    
def writeToCSV(output, business_id):
    with open('{}.csv'.format(business_id), 'a') as f: #change filename
        writer = csv.writer(f)
        for annotated_review in output:
            chunk = annotated_review['ner_chunk']
            for token in chunk:
                writer.writerow([business_id, \
                                token.result, \
                                token.metadata['entity']])
    return None
    
def processManyBusinesses(ids):
    ''' takes list of business ids, 
    finds reviews,
    annotates reviews,
    writes them to csv'''
    for b_id in ids:
        reviews = getReviews(b_id)
        print('Annotating reviews for ID: {}'.format(b_id))
        annotated_reviews = annotateReviews(reviews, lightPipeline)
        print('Finished annotating reviews for {}, saving... '.format(b_id))
        writeToCSV(annotated_reviews, b_id)
    return None

# example
# business_id = reviews_MA.business_id[0]
# text_input = getReviews(business_id)
# results = annotateReviews(text_input, lightPipeline)
# writeToCSV(results, business_id)

if __name__ == "__main__":
    reviews_MA = pd.read_csv('reviews_MA_full.csv.gz', compression='gzip')
    grouped = reviews_MA.groupby('business_id')
    counts = grouped['review_id'].agg(['count'])\
        .sort_values('count', ascending=False)
    spark = sparknlp.start()
    pipeline = createPipeline()
    lightPipeline = initializeModel(pipeline)
    processManyBusinesses(counts.index[3:5])
