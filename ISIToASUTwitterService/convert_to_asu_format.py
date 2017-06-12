from flask import Flask, request, jsonify, Response
import glob
from dateutil.parser import parse
import json
import os
import tarfile
import datetime

inputDirectory = "/Users/osuba/Documents/Studies/ISI/EFFECT/onzali/effect-twitter-data-conversion/data" #"/nfs/topaz/annas/bin/cause-effect/twitter/streamdata"

app = Flask(__name__)

class GetTwitterData(object):
    def __init__(self, count=0, results=[], type="getTwitterData", message="SUCCESS"):
        self.type=type
        self.message=message
        self.count=count
        self.results=results

class ASUTwitterData(object):
    def __init__(self,userName,tweetContent,retweet,favorites,recorded_time_seconds,tweet_id,conversation_id,content_type="Tweet",hashtags="",mentions=""):
        self.content_type=content_type
        self.userName=userName
        self.tweetContent=tweetContent
        self.hashtags=hashtags
        self.retweet=retweet
        self.favorites=favorites
        self.recorded_time_seconds=recorded_time_seconds
        self.tweet_id=tweet_id
        self.conversation_id=conversation_id
        self.mentions=mentions

def jsonDefault(object):
    return object.__dict__

@app.route('/getTwitterData', methods=['GET'])
def getTwitterData():
    limit = int(request.args.get('limit')) if request.args.get('limit') else 10
    start = int(request.args.get('start')) if request.args.get('start') else 0
    retweet_count = request.args.get('retweet')
    userName = request.args.get('userName')
    tweet_id = request.args.get('tweet_id') if request.args.get('tweet_id') else None
    conversation_id = request.args.get('conversation_id') if request.args.get('conversation_id') else None
    fromDate = request.args.get('from')
    # if parameter 'to' is not provided, take today's date
    toDate = request.args.get('to') if request.args.get('to') else datetime.datetime.today().strftime("%Y-%m-%d")
    hashTag = request.args.get('hashTag')
    results = []
    results = convertToASUFormat(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id,hashTag)
    count=len(results)
    results=results[start:start+limit] if start+limit < count else results[start:count]
    return Response(json.dumps(GetTwitterData(count,results),default=jsonDefault,indent=1), mimetype='application/json')

def getFile(inputDirectory,inputFile):
    corrupted=False
    fileName = os.path.splitext(os.path.basename(inputFile))[0]
    if fileName.endswith("tar"):
        tar = tarfile.open(inputFile)
        try:
            members=tar.getmembers()
            f=tar.extractfile(tar.getmembers()[0])
            fileName=fileName.split(".")[0]
        except Exception as e:
            print(e)
            try:
                tar = tarfile.open(inputFile, "r:gz")
                tar.extractall(inputDirectory)
            except Exception as ex:
                print(ex)
                fileName=fileName.split(".")[0]
                f=open(inputDirectory+"/"+fileName+".json")
                corrupted=True
    else:
        f=open(inputFile)
    return f,fileName,corrupted

def convertToASUFormat(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag):

    results=[]
    dataFiles = glob.glob(inputDirectory+"/*")
    dataFiles.sort()
    # if parameter 'from' is not provided
    if fromDate is not None:
        for inputFile in dataFiles:
            corrupted=False
            f,fileName,corrupted=getFile(inputDirectory,inputFile)
            if parse(fileName) >= parse(fromDate) and parse(fileName) <= parse(toDate):
                results.extend(getResults(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, f))
                if corrupted:
                    os.remove(inputDirectory+"/"+fileName+".json")
    else:
        for inputFile in dataFiles:
            f,fileName,corrupted=getFile(inputDirectory,inputFile)
            # fileName = os.path.splitext(os.path.basename(inputFile))[0]
            # if fileName.endswith("tar.gz"):
            #     tar = tarfile.open(fileName)
            #     f=tar.extractfile(member)
            # else:
            #     f=open(inputFile)
            if parse(fileName) <= parse(toDate):
                result.extend(getResults(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, f))
    return results

def getResults(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, inputFile):
    count = 0
    startRecord=0
    appendBool = False
    data = []

    #filter coditions
    if retweet_count or userName or tweet_id or conversation_id or hashTag:
        for line in inputFile:
	    try:
                jsonObject=json.loads(line)
	    except Exception as e:
		print(e)
		continue
            if 'id' in jsonObject:
                if retweet_count is not None:
                    if jsonObject['retweet_count']==retweet_count:
                        appendBool=True
                if userName is not None:
                    if jsonObject['user']['screen_name']==userName:
                        appendBool=True
                if tweet_id is not None:
                    if jsonObject['id_str']==tweet_id:
                        appendBool=True
                if conversation_id is not None:
                    if jsonObject['in_reply_to_status_id_str']==conversation_id:
                        appendBool=True
                if hashTag is not None:
                    hashTags=jsonObject['entities']['hashtags']
                    if hashTags is not None:
                        for tag in hashTags:
                            if hashTag in tag['text']:
                                appendBool=True
                                break;
                if appendBool:
                    data.append(jsonObject)
                    appendBool=False
    else:
        for line in inputFile:
	    try:
                jsonObject=json.loads(line)
	    except Exception as e:
		print(e)
		continue
            data.append(jsonObject)
    return getConvertedData(data)

def getConvertedData(data):
    convertedData=[]

    for d in data:
        hashtags=[]
        mentions=[]
        if 'entities' in d:
            hashtagsArray=d['entities']['hashtags']
            mentionsArray=d['entities']['user_mentions']
            if len(hashtagsArray)>0:
                for h in hashtagsArray:
                    hashtags.append("u'"+h['text']+"'")
            hashtagsStr=", ".join(hashtags)

            if len(mentionsArray)>0:
                for m in mentionsArray:
                    mentions.append("u'"+m['screen_name']+"'")
            mentionsStr=", ".join(mentions)

            parsed_date = parse(d['created_at'])

            asuTwitterData=ASUTwitterData(userName="@"+d['user']['screen_name'], tweetContent=d['text'],
                                     retweet=d['retweet_count'],favorites=d['favorite_count'],
                                     recorded_time_seconds=parsed_date.strftime("%a %b %d %H:%M:%S UTC %Y"), tweet_id=d['id_str'],
                                    conversation_id=(d['in_reply_to_status_id_str'] if d['in_reply_to_status_id_str'] is not None else d['id_str']),hashtags=hashtagsStr,mentions=mentionsStr)
            convertedData.append(asuTwitterData)
    return convertedData

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
