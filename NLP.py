# Just for
import numpy as np
import networkx as nx
import pandas as pd
import json

# from preprocessor
import datetime
import yaml
import os
import community
import functions as fn

#For NLP
import spacy
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
import re
#import en_core_web_sm
#import contractions
import functions as fn
import yaml

from contractions import CONTRACTION_MAP
import unicodedata
# initialize afinn sentiment analyzer
from afinn import Afinn

#Set up NLP stuff
af = Afinn()
nlp = spacy.load('en_core_web_sm', parse=True, tag=True, entity=True)
#nlp = en_core_web_sm.load()
tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
stopword_list.remove('no')
stopword_list.remove('not')

def expand_contractions(text, contraction_mapping=CONTRACTION_MAP):
    contractions_pattern = re.compile('({})'.format('|'.join(contraction_mapping.keys())),
                                      flags=re.IGNORECASE | re.DOTALL)

    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = contraction_mapping.get(match) \
            if contraction_mapping.get(match) \
            else contraction_mapping.get(match.lower())
        expanded_contraction = first_char + expanded_contraction[1:]
        return expanded_contraction

    expanded_text = contractions_pattern.sub(expand_match, text)
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text

def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text

def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text

def simple_stemmer(text):
    ps = nltk.porter.PorterStemmer()
    text = ' '.join([ps.stem(word) for word in text.split()])
    return text

def lemmatize_text(text):
    text = nlp(text)
    text = ' '.join([word.lemma_ if word.lemma_ != '-PRON-' else word.text for word in text])
    return text

def remove_stopwords(text, tokenizer, stopword_list, is_lower_case=False):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text

def normalize_corpus(corpus, tokenizer, stopword_list, contraction_expansion=True,
                     accented_char_removal=True, text_lower_case=True,
                     text_lemmatization=True, special_char_removal=True,
                     stopword_removal=True, remove_digits=True):
    normalized_corpus = []
    # normalize each document in the corpus
    for doc in corpus:
        # remove accented characters
        if accented_char_removal:
            try:
                doc = remove_accented_chars(doc)
            except:
                doc = doc
        # expand contractions
        if contraction_expansion:
            try:
                doc = expand_contractions(doc)
            except:
                doc = doc
                # lowercase the text
        if text_lower_case:
            try:
                doc = doc.lower()
            except:
                doc = doc
        # remove extra newlines
        try:
            doc = re.sub(r'[\r|\n|\r\n]+', ' ', doc)
        except:
            doc = doc
        # lemmatize text
        if text_lemmatization:
            try:
                doc = lemmatize_text(doc)
            except:
                doc = doc
        # remove special characters and\or digits
        if special_char_removal:
            try:
                # insert spaces between special characters to isolate them
                special_char_pattern = re.compile(r'([{.(-)!}])')
                doc = special_char_pattern.sub(" \\1 ", doc)
                doc = remove_special_characters(doc, remove_digits=remove_digits)
            except:
                doc = doc
        # remove extra whitespace
        try:
            doc = re.sub(' +', ' ', doc)
        except:
            doc = doc
        # remove stopwords
        if stopword_removal:
            try:
                doc = remove_stopwords(doc, tokenizer, stopword_list, is_lower_case=text_lower_case)
            except:
                doc = doc

        normalized_corpus.append(doc)

    return normalized_corpus

def buildCommunityData(nodes, topGroups, nlp, af, tokenizer, stopword_list, df):
    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    communities_path = 'data/' + str(parameters['project']) + '/communities/'
    if not os.path.exists(communities_path):
        os.makedirs(communities_path)
    project = parameters['project']
    out_path = 'data/' + str(project) + '/communities/'
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    df1 = pd.DataFrame(columns=['group', 'influencers', 'sentiment', 'top_entities'])
    for group in topGroups:
        # pre-process text and store the same
        temp = nodes.loc[nodes["group"] == group]
        temp['clean_text'] = normalize_corpus(temp['tweet_text'], tokenizer, stopword_list)
        norm_corpus = list(temp['clean_text'])

        # create a basic pre-processed corpus, don't lowercase to get POS context
        corpus = normalize_corpus(temp['tweet_text'], tokenizer, stopword_list, text_lower_case=False,
                                  text_lemmatization=False, special_char_removal=False)

        named_entities = []
        for sentence in corpus:
            temp_entity_name = ''
            temp_named_entity = None
            sentence = nlp(sentence)
            for word in sentence:
                term = word.text
                tag = word.ent_type_
                if tag:
                    temp_entity_name = ' '.join([temp_entity_name, term]).strip()
                    temp_named_entity = (temp_entity_name, tag)
                else:
                    if temp_named_entity:
                        named_entities.append(temp_named_entity)
                        temp_entity_name = ''
                        temp_named_entity = None

        entity_frame = pd.DataFrame(named_entities,
                                    columns=['Entity Name', 'Entity Type'])

        # get the top named entities
        top_entities = (entity_frame.groupby(by=['Entity Name', 'Entity Type'])
                        .size()
                        .sort_values(ascending=False)
                        .reset_index().rename(columns={0: 'Frequency'}))
        # top_entities.T.iloc[:,:15]

        # compute sentiment scores (polarity) and labels
        sentiment_scores = [af.score(article) for article in corpus]
        sentiment_category = ['positive' if score > 0
                              else 'negative' if score < 0
        else 'neutral'
                              for score in sentiment_scores]

        # sentiment
        sentiment = sum(sentiment_scores) / len(sentiment_scores)

        # influencers
        temp = temp.sort_values(['degree'], ascending=False)
        influencers = list(temp['name'][:5])

        # Top 5 entities
        top5entities = list(top_entities['Entity Name'][:5])
        if len(top5entities) < 5:
            pad = 5 - len(top5entities)
            top5entities.extend([0] * pad)

        # sentiment statistics per news category
        df2 = pd.DataFrame(columns=['group', 'influencers', 'sentiment', 'top_entities', 'date'])
        df2['influencers'] = influencers
        df2['sentiment'] = sentiment
        if len(top5entities) == 5:
            df2['top_entities'] = top5entities
        else:
            df2["top_entities"] = 0
        df2['group'] = group
        df2["date"] = timestamp
        df1 = df.append(df2)
        #Should this be df1?
        # Write to csv with datetime stamp

    df1.to_csv(str(out_path) + "communities" + timestamp + ".csv")  # MacOS path


def NLP(nodes, df):
    # Set up NLP stuff

    af = Afinn()
    nlp = spacy.load('en_core_web_sm', parse=True, tag=True, entity=True)
    # nlp = en_core_web_sm.load()
    tokenizer = ToktokTokenizer()
    stopword_list = nltk.corpus.stopwords.words('english')
    stopword_list.remove('no')
    stopword_list.remove('not')

    topGroups = nodes['group'].unique().tolist()
    print(topGroups)

    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    NLP_path = 'data/' + str(parameters['project']) + '/NLP/'
    if not os.path.exists(NLP_path):
        os.makedirs(NLP_path)
    project = parameters['project']
    out_path = 'data/' + str(project) + '/NLP/'
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    #nodes.to_csv("test.csv")  # MacOS path

    df1 = pd.DataFrame(columns=['group', 'influencers', 'sentiment', 'top_entities'])
    for group in topGroups:
        # pre-process text and store the same
        temp = nodes.loc[nodes["group"] == group]
        temp['clean_text'] = normalize_corpus(temp['tweet_text'], tokenizer, stopword_list)
        #norm_corpus = list(temp['clean_text'])

        # create a basic pre-processed corpus, don't lowercase to get POS context
        corpus = normalize_corpus(temp['tweet_text'], tokenizer, stopword_list, text_lower_case=False,
                                  text_lemmatization=False, special_char_removal=False)

        named_entities = []
        for sentence in corpus:
            temp_entity_name = ''
            temp_named_entity = None
            sentence = nlp(sentence)
            for word in sentence:
                term = word.text
                tag = word.ent_type_
                if tag:
                    temp_entity_name = ' '.join([temp_entity_name, term]).strip()
                    temp_named_entity = (temp_entity_name, tag)
                else:
                    if temp_named_entity:
                        named_entities.append(temp_named_entity)
                        temp_entity_name = ''
                        temp_named_entity = None

        entity_frame = pd.DataFrame(named_entities,
                                    columns=['Entity Name', 'Entity Type'])

        # get the top named entities
        top_entities = (entity_frame.groupby(by=['Entity Name', 'Entity Type'])
                        .size()
                        .sort_values(ascending=False)
                        .reset_index().rename(columns={0: 'Frequency'}))
        # top_entities.T.iloc[:,:15]

        # compute sentiment scores (polarity) and labels
        sentiment_scores = [af.score(article) for article in corpus]
        sentiment_category = ['positive' if score > 0
                              else 'negative' if score < 0
        else 'neutral'
                              for score in sentiment_scores]

        # sentiment
        sentiment = sum(sentiment_scores) / len(sentiment_scores)

        # influencers
        temp = temp.sort_values(['degree'], ascending=False)
        influencers = list(temp['name'][:5])

        if len(influencers) < 5:
            pad = 5 - len(influencers)
            influencers.extend([0] * pad)

        # Top 5 entities
        top5entities = list(top_entities['Entity Name'][:5])
        if len(top5entities) < 5:
            pad = 5 - len(top5entities)
            top5entities.extend([0] * pad)

        # sentiment statistics per news category
        df2 = pd.DataFrame(columns=['group', 'influencers', 'sentiment', 'top_entities', 'date'])
        df2['influencers'] = influencers

        df2['sentiment'] = sentiment
        if len(top5entities) == 5:
            df2['top_entities'] = top5entities
        else:
            df2["top_entities"] = 0
        df2['group'] = group
        df2["date"] = timestamp

        #df.to_csv("test.csv")
        df2['tweet_text'] = df2['influencers'].apply(lambda x: fn.getText1(x, df))
        df2['tweet_id'] = df2['influencers'].apply(lambda x: fn.getTweetID(x, df))
        df2['number_of_retweets'] = df2['influencers'].apply(lambda x: fn.getMaxRTs(x, df))

        df1 = df1.append(df2)
        #Should this be df1?
        # Write to csv with datetime stamp

    df1.to_csv(str(out_path) + "communities" + timestamp + ".csv")  # MacOS path
