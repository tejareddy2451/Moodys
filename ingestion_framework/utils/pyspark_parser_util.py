import csv  
import uuid
import pyarrow.parquet as pq
import boto3
import io
import sys


def main():
    domain=sys.argv[1]
    env=sys.argv[2]
    region=sys.argv[3]
    print("Hello World!")
    readPySpark('transform.py',domain,env,region)
    print('done')

def readPySpark(file,domain,env,region):
    array=[]
    readTxt=''
    readarray=[]
    readBoolean=False
    arrayVariable=[]
    dict={}
    f = open(file, "r")
    while(True):
        line = f.readline()
        if not line:
            break

        else:
            if '.write' in line.lower():
                array.append(line.replace('\n',''))
            elif '.read' in line.lower():
                if readBoolean:
                    readarray.append(readTxt)
                readBoolean=False
                #print("read")
                readTxt=line.replace('\n','')
                readBoolean=True
                continue
            elif readBoolean:
                #print("true")
                readTxt+=line.replace('\n','')
            if '=' in line:
                if readBoolean:
                    readarray.append(readTxt)
                readBoolean=False
                splitVal=line.split('=')
                dict[splitVal[0].strip()]=splitVal[1].strip().replace('\n','')
    
    
    for txtdata in readarray:
        updated_data=txtdata.strip().replace("\\","").replace(" ","")
        print(updated_data)
        loadTxt=updated_data[updated_data.index('load')+5:len(updated_data)]
        
        reads3path=loadTxt[1:loadTxt.index("format")]
        reads3path=reads3path.replace("'",'').replace('.','').replace("\"",'')

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    tableStatus = dynamodb.Table("datamesh-pyspark-output-table-dev")

    header=['UUID','File_Name','Type','Target_Table_Name','S3_Location','Column_Details']
    csvFullData=[]
    csvData=[]
    i=1
    for data in array:
        csvData=[]
        UUID=str(uuid.uuid4()).replace("\"","")
        csvData.append(UUID)
        csvData.append("edw_location_transform")
        csvData.append("Write")
        savetext=data[data.index('save')+5:len(data)-1]
        if 's3' in savetext:
            if 'format' in savetext:
                print('format')
                s3Path=savetext[1:savetext.index('.format')]
                print(s3Path)
            else:
                s3Path=savetext
        else:
            if dict.get(savetext) is not None:
                s3Path=dict.get(savetext).replace("\"","")
        
        tablename=s3Path.replace("'","").split('/')[len(s3Path.split('/'))-2]
        print(tablename)
        csvData.append(tablename)
        csvData.append(s3Path.replace("'","").replace("s3a","s3"))
        bucketname=dict.get('bucket_name')
        stringJSON=getJSON(s3Path,tablename,domain,env,bucketname,region)
        csvData.append(stringJSON)
        csvFullData.append(csvData)
        tableStatus.put_item(
            Item={
                    'UUID': str(UUID),
                    'File_Name':"edw_location_transform",
                    'Type':"Write",
                    'Target_Table_Name':str(tablename),
                    'S3_Location':str(s3Path.replace("'","").replace("s3a","s3")),
                    'Column_Details':str(stringJSON)
                    }
                )
            
        i=i+1
    #print(dict)
    for txtdata in readarray:
        csvData=[]
        updated_data=txtdata.strip().replace("\\","").replace(" ","")
        print(updated_data)
        loadTxt=updated_data[updated_data.index('load')+5:len(updated_data)]
        
        reads3path=loadTxt[1:loadTxt.index("format")]
        reads3path=reads3path.replace("'",'').replace('.','').replace("\"",'')

        UUID=str(uuid.uuid4()).replace("\"","")
        csvData.append(UUID)
        csvData.append("edw_location_transform")
        csvData.append("Read")
        
        s3Path=reads3path
        print("s3Path")
        print(s3Path)
        tablename=s3Path.replace("'","").split('/')[len(s3Path.split('/'))-1]
        if tablename=='':
            tablename=s3Path.replace("'","").split('/')[len(s3Path.split('/'))-2]
        print(tablename)
        csvData.append(tablename)
        csvData.append(s3Path.replace("'","").replace("s3a","s3"))
        bucketname=dict.get('bucket_name')
        stringJSON=getJSON(s3Path,tablename,domain,env,bucketname,region)
        csvData.append(stringJSON)
        csvFullData.append(csvData)
        tableStatus.put_item(
            Item={
                    'UUID': str(UUID),
                    'File_Name':"edw_location_transform",
                    'Type':"Read",
                    'Target_Table_Name':str(tablename),
                    'S3_Location':str(s3Path.replace("'","").replace("s3a","s3")),
                    'Column_Details':str(stringJSON)
                    }
                )

    f.close

    #header=["S.No,File_Name,Type,Target_Table_Name,S3_Location"]
    #print(csvFullData)
    with open('details1.csv', 'w', encoding='UTF8', newline='') as fw:
        writer = csv.writer(fw)
        writer.writerow(header)
        writer.writerows(csvFullData)
    fw.close

def getJSON(s3Path,tablename,domain,env,bucketName,region):
    s3Path=s3Path.replace("'","")
    bucketName=bucketName.replace("{domain_name}",domain).replace("{env}","231376005945-"+env).replace("{aws_region}",region).replace("f\"","").replace("\"","")
    print(bucketName)
    buffer = io.BytesIO()
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucketName)
    #print(s3Path.index('data'))
    dir=s3Path[s3Path.index('data'):len(s3Path)]
    print(dir)

    for object_summary in my_bucket.objects.filter(Prefix=dir):
        print(object_summary.key)
        s3_object = s3.Object(bucketName, object_summary.key)
        s3_object.download_fileobj(buffer)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        columnlist=[]
        typelist=[]
        typelistFinal=[]
        i=0
        stringJSON='{"ColumnList" : ['
        #print(df)
        for col in df.columns:
            columnlist.append(col)
        for type in df.dtypes:
            typelist.append(type)
        

        for t in typelist:
            if 'object' in str(t):
                typelistFinal.append("String")
            if 'int' in str(t):
                typelistFinal.append("Int")
            if 'float' in str(t):
                typelistFinal.append("Float")
            if 'datatime' in str(t):
                typelistFinal.append("DateTime")
            else:
                typelistFinal.append(str(t))

        while i < len(columnlist):
            #print(columnlist[i])
            #print(typelist[i])
            if(i==len(columnlist)-1):
                stringJSON+="{\"Column_Name\":\""+columnlist[i]+"\",\"Type\":\""+str(typelistFinal[i])+"\"}"
            else:
                stringJSON+="{\"Column_Name\":\""+columnlist[i]+"\",\"Type\":\""+str(typelistFinal[i])+"\"},"
            i+=1

        stringJSON+="]}"
        break
    return stringJSON


if __name__ == "__main__":
    main()