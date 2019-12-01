import requests
from urllib.parse import urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup
import sys
import csv
import pymysql
def get_url_content(url):#获取urlhtml内容，返回html源码
    response=requests.get(url)
    if response.status_code==200:
        if "抱歉，指定的主题不存在或已被删除或正在被审核" in response.text:
            return False
        else:
            return response.text
    else:
        return False
def parse_post_data(html_text):#获取发帖的所要爬取的内容
    soup_object=BeautifulSoup(html_text,'html5lib')
    get_user_img(soup_object)
    user_list=get_post_userlist(soup_object)
    content_list=get_post_content_list(soup_object)
    part=soup_object.title.string.split(" - ")[1]
    post_data_list={}
    for i in range(len(content_list)):
        post_data_list[i]={
            "part":part,
            "uid":user_list[i]['uid'],
            "user_name":user_list[i]['user_name'],
            "tid":content_list[i]['tid'],
            "content":content_list[i]['content']
        }
    return post_data_list
def get_post_content_list(post_soup_object):#获取tid和发帖内容
    content_object=post_soup_object.select('.t_f')
    content_list=[]
    for i in range(len(content_object)):
        tid=content_object[i]['id'].split("_")[1]
        content=content_object[i].string
        content_list.append({"tid":tid,"content":content})
    return content_list
def get_post_userlist(post_soup_object):#获取用户名和uid
    user_object=post_soup_object.select('.authi')
    user_list=[]
    for i in range(len(user_object)):
        if i %2==0:
            user_name=user_object[i].a.string
            uid=parse_qs(user_object[i].a['href'])['uid'][0]
            user_list.append({"user_name":user_name,"uid":uid})
    return user_list
def get_user_img(post_soup_object):#获取用户头像
    imgall=post_soup_object.select('.avatar')
    for i in range(len(imgall)):
        imgurl=imgall[i].a.img['src']
        img=requests.get(imgurl)
        pic=imgurl.split("/")[8]
        picpath="F:\\asdf\\img\\"+pic+".jpg"
        if img.status_code==200:
            f=open(picpath,'wb')
            f.write(img.content)
    f.close()
max_tid=sys.argv[1]
post_base_url="https://discuz.net/forum.php?mod=viewthread&tid="
f=open("F:\\asdf\\asdf.csv",'a',encoding='utf-8-sig')
csv_w=csv.writer(f)
csv_w.writerow(["版块","uid","用户名","tid","发帖内容"])
conn=pymysql.connect("192.168.88.134","root","123456","data")
cursor=conn.cursor()
for i in range(int(max_tid)):
    ret=get_url_content(post_base_url+str(i))
    if ret!=False:
        parsed_post_data=parse_post_data(ret)
        print("got post data tid:%s"%i)
        for j in range(len(parsed_post_data)):
            part=parsed_post_data[j]['part']
            user_name = parsed_post_data[j]['user_name']
            uid = parsed_post_data[j]['uid']
            tid = parsed_post_data[j]['tid']
            content = parsed_post_data[j]['content']
            if content==None:
                content="None"
            else:
                content=content.replace("'","")
            cursor.execute("insert into data(part,uid,user_name,tid,content) values('"+part+"',"+str(uid)+",'"+user_name+"',"+str(tid)+",'"+content+"');")
            conn.commit()
            csv_w.writerow([part,uid,user_name,tid,content])
    else:
        print("tid:%s not found ,continue"%i)
cursor.close()
conn.close()
f.close()