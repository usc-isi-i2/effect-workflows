from flask import Flask, request, jsonify, Response
import glob
from dateutil.parser import parse
import json
import os
import tarfile
import datetime

inputDirectory = "/nfs/topaz/annas/bin/cause-effect/twitter/streamdata"

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
    args=request.args
    limit = int(args.get('limit')) if args.get('limit') else 10
    start = int(args.get('start')) if args.get('start') else 0
    retweet_count = args.get('retweet')
    userName = args.get('userName')
    tweet_id = args.get('tweetId') if args.get('tweetId') else None
    conversation_id = args.get('conversationId') if args.get('conversationId') else None
    fromDate = args.get('from')
    # if parameter 'to' is not provided, take today's date
    toDate = args.get('to') if args.get('to') else datetime.datetime.today().strftime("%Y-%m-%d")
    hashTag = args.get('hashTag')
    results = []
    count,results = convertToASUFormat(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id,hashTag)
    return Response(json.dumps(GetTwitterData(count,results),default=jsonDefault,indent=1), mimetype='application/json')

def getFile(inputDirectory,inputFile):
    fileName = os.path.splitext(os.path.basename(inputFile))[0]
    if fileName.endswith("tar"):
        tar = tarfile.open(inputFile)
        try:
            members=tar.getmembers()
            f=tar.extractfile(tar.getmembers()[0])
            fileName=fileName.split(".")[0]
        except Exception as e:
            print(fileName+": "+str(e))
            return None,fileName
    else:
        f=open(inputFile)
    return f,fileName

def convertToASUFormat(start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag):
    totalCount=0
    startCount=0
    results=[]
    types = ('*.json', '*.tar.gz')
    dataFiles = []
    for files in types:
        dataFiles.extend(glob.glob(inputDirectory+"/"+files))
    dataFiles.sort()
    
    prevFileName=""
    # if parameter 'from' is not provided
    if fromDate is not None:
        for inputFile in dataFiles:
            f,fileName=getFile(inputDirectory,inputFile)
            if prevFileName==fileName:
                continue
            prevFileName=fileName
            if f is not None: 
                count=0
                res=[]
                if parse(fileName) >= parse(fromDate) and parse(fileName) <= parse(toDate):
                    startCount,count,res=getResults(startCount, start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, f)
                    results.extend(res)
                    totalCount+=count
    else:
        for inputFile in dataFiles:
            f,fileName=getFile(inputDirectory,inputFile)
            if prevFileName==fileName:
                continue
            prevFileName=fileName
            if f is not None: 
                if parse(fileName) <= parse(toDate):
                    startCount,count,res=getResults(startCount, start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, f)
                    results.extend(res)
                    totalCount+=count
    return totalCount,results

def getResults(startCount, start, limit, fromDate, toDate, retweet_count, userName, tweet_id, conversation_id, hashTag, inputFile):
    count = 0
    appendBool = False
    data = []

    #filter coditions
    if retweet_count or userName or tweet_id or conversation_id or hashTag:
        for line in inputFile:
            try:
                jsonObject=json.loads(line)
            except Exception as e:
                print(str(e)+": "+inputFile.name+": "+line)
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
                    conversationId=jsonObject['in_reply_to_status_id_str']
                    if jsonObject['in_reply_to_status_id_str'] is None:
                        conversationId=jsonObject['id_str']
                    if conversationId==conversation_id:
                        appendBool=True
                if hashTag is not None:
                    hashTags=jsonObject['entities']['hashtags']
                    if hashTags is not None:
                        for tag in hashTags:
                            if hashTag in tag['text']:
                                appendBool=True
                                break;
                if appendBool:
                    startCount+=1
                    count+=1
                    appendBool=False
                    if startCount>start and len(data)<limit:
                        data.append(jsonObject)
                    
    else:
        for line in inputFile:
    	    try:
                jsonObject=json.loads(line)
                startCount+=1
                count+=1
    	    except Exception as e:
    	        print(str(e)+": "+inputFile.name+": "+line)
    	        continue
            if startCount>start and len(data)<limit:
                data.append(jsonObject)
    return startCount,count,getConvertedData(data)

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
