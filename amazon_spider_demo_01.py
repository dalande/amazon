import pymssql
import requests
from lxml import etree
import time
from threading import Timer
from logger import MyLog
import configparser
import os
import random

root_dir = os.path.dirname(os.path.abspath(__file__))  # 获取当前文件所在目录
cf = configparser.ConfigParser()
cf.read(root_dir + "\config.ini")  # 读取配置文件
secs = cf.sections()
SQL_SERVER = cf.get("Database", "SQL_SERVER")
SQL_USER = cf.get("Database", "SQL_USER")
SQL_PASSWORD = cf.get("Database", "SQL_PASSWORD")
SQL_NAME = cf.get("Database", "SQL_NAME")
WAIT_REQUEST_TIME = int(cf.get("Constant", "WAIT_REQUEST_TIME"))  # 发送请求间隔
MAX_SPIDER_PAGE = int(cf.get("Constant", "MAX_SPIDER_PAGE"))  # 最大爬取页数
MAX_RANK = int(cf.get("Constant", "MAX_RANK"))  # 超过20页还爬取不到该商品，排名设置为300
SPIDER_INTERVAL = int(cf.get("Constant", "SPIDER_INTERVAL"))  # 每一轮爬取时间间隔
COOKIE = cf.get("Constant", "COOKIE")


def get_ua_proxy_random():
    """获取代理ip"""
    conn = pymssql.connect(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_NAME)
    cursor = conn.cursor()
    # 获取随机代理ip
    sql = "select top 1 * from Table_Proxy order by NEWID()"
    cursor.execute(sql)
    proxy_ip = cursor.fetchone()[1]
    # 获取随机ua
    sql = 'select top 1 * from Table_UserAgent order by NEWID()'
    cursor.execute(sql)
    ua = cursor.fetchone()[1]
    cursor.close()
    conn.close()

    return ua, proxy_ip


def get_all_record_list():
    # 按id升序获取表A中所有记录的列表
    conn = pymssql.connect(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_NAME)
    cursor = conn.cursor()
    sql = "select Asin,Keyword from Table_Asin order by id asc"
    cursor.execute(sql)
    all_record_list = cursor.fetchall()
    cursor.close()
    conn.close()
    return all_record_list


def disdinct(all_record_list):
    """列表去重"""
    valid_record_list = []
    # print('所有记录', all_record_list)
    for i in all_record_list:
        if i not in valid_record_list:
            valid_record_list.append(i)
    # print('去重后的记录', valid_record_list)
    return valid_record_list


def get_html(keyword_list, page):
    # 爬取列表页面
    ua, proxy_ip = get_ua_proxy_random()
    headers = {
        "User-Agent": ua,
        "Cookie": COOKIE,
    }

    proxies = {"http": "http://" + proxy_ip, }

    url_detail = "https://www.amazon.com/s?k="
    keyword_num = len(keyword_list)
    current_num = 0
    while current_num < keyword_num:
        url_detail += keyword_list[current_num] + "+"
        current_num += 1
    url_detail += "&page=%d" % page
    mylog.info("当前爬取地址:%s" % url_detail)
    try:
        response = requests.get(url_detail, headers=headers, proxies=proxies, timeout=(3, 7))
        mylog.info("列表页爬取成功")
    except:
        mylog.error("列表页爬取失败")
        return None
    list_html = response.text

    # list_html = location(url_detail)
    return list_html


def parse_html(asin_str, html):
    # 提取数据，获取相应asin所对应的排名
    selector = etree.HTML(html)
    asin_list = selector.xpath("//div[@class='s-result-list s-search-results sg-row']/div/@data-asin")
    delivery_area = selector.xpath('//span[@id="glow-ingress-line2"]/text()')
    mylog.info("配送地址:%s" % delivery_area)
    # 去除列表中的空字符串
    asin_list = list(filter(None, asin_list))
    asin_num = len(asin_list)
    if asin_num == 0:
        return 0, None
    if asin_str not in asin_list:
        return None, None
    rank = asin_list.index(asin_str) + 1
    return asin_num, rank


def save_rank(asin_str, keyword_str, rank):
    # 数据存储
    conn = pymssql.connect(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_NAME)
    cursor = conn.cursor()
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sql = "insert into Table_Rank(Asin, Keyword, Rank, Datetime) values (%s,%s,%d,%s)"
    data = [(asin_str, keyword_str, rank, current_time)]
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()


def main():
    # record_num = get_record_num()  # 获取表A中记录数量
    all_record_list = get_all_record_list()
    valid_record_list = disdinct(all_record_list)
    record_num = len(valid_record_list)
    for cureent_num in range(record_num):
        # 获取asin和kw
        asin_str, keyword_str = valid_record_list[cureent_num]
        keyword_list = keyword_str.split(' ')

        current_page = 1
        # 最多爬取20页，
        while current_page < MAX_SPIDER_PAGE:
            mylog.info("当前爬取的asin:%s" % asin_str)
            list_html = get_html(keyword_list, current_page)
            if list_html is None:
                # 爬取网页失败
                break

            asin_num, rank = parse_html(asin_str, list_html)
            # asin_num == 0,表明该页没有商品数据
            if asin_num == 0:
                save_rank(asin_str, keyword_str, None)
                break

            if rank is None:
                # 当页不存在该asin时请求下一页
                current_page += 1
                if current_page == MAX_SPIDER_PAGE:
                    # 如果到了第20页还查不到该asin，则排名设置为300
                    save_rank(asin_str, keyword_str, MAX_RANK)
                    break
                time.sleep(WAIT_REQUEST_TIME)
                continue
            # 每一页的asin数量*页数 + 当前页面的rank
            rank = (current_page - 1) * asin_num + rank
            mylog.info("爬取到的排名:%d" % rank)
            save_rank(asin_str, keyword_str, rank)
            break
        time.sleep(WAIT_REQUEST_TIME)

    mylog.info("当前正在爬取的是列表页rank,等待下一轮爬取,间隔1h......")
    t1 = Timer(SPIDER_INTERVAL, main)
    t1.start()


if __name__ == '__main__':
    mylog = MyLog()
    main()
