# -*- coding:utf-8 -*-
import time
import datetime
import json
import socket
import glob
import requests
import smtplib
import re
import redis
# import MySQLdb
import logging
import logging.handlers
from email.mime.text import MIMEText
from express02_companys import PackageItem
from express02_companys import ExpressCompany

# 172.17.21.83
#123.59.15.27:92
# URL_KUAIDI = 'http://116.62.132.116:85/get_express_crawl_list?count=100'
URL_UPDATE = 'http://47.95.50.108:92/express_crawl_callback'
# demo mysql
# URL_KUAIDI = 'http://116.62.132.116:85/get_express_crawl_list?count=100&page='

#server redis
red = redis.Redis(host='172.17.21.83', port=6379)
#demo redis
# red = redis.Redis(host='192.168.133.1', port=6379)


EXPIRED_TIME = 2*60*60  # 单位秒
REQUEST_DELAY_TIME = 25
LOGGING_FILE = 'express_db.log'
MAX_RETRY_TIME=5 #最多重试次数

logging.basicConfig()
logger = logging.getLogger('express_db')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)
# handler = logging.handlers.TimedRotatingFileHandler(LOGGING_FILE, when='M',interval=1, backupCount=5)
handler = logging.handlers.RotatingFileHandler(LOGGING_FILE,maxBytes=20971520,backupCount=5)
handler.setFormatter(formatter)
# handler.suffix="%Y%m%d-%H%M.log"
logger.addHandler(handler)


def err_update(data,create_time):
    # 当更新失败时 再次将该条数据存入redis
    update_times = datetime.datetime.now()
    update_times = update_times.strftime('%Y-%m-%d %H:%M:%S')
    again_data = {"id": data['id'], "package_no": data['package_no'], "company_code": data['company_code'],
                  'update_time': update_times, 'create_time': create_time}
    # 在redis的右侧队列添加新的元素
    red.rpush('datas', again_data)


def update_express_info(id,package_no,company_code,update_time,create_time,express_status,express_detail):
    """
    提交更新数据
    :param id:
    :param trade_no:
    :param transport_detail:
    :return:
    """
    # print '进入更新入库流程'
    data = {"id": id, "package_no": package_no,"company_code":company_code,"express_status":express_status, "express_detail": express_detail}
    try:
        r = requests.post(URL_UPDATE, data=json.dumps(data))
        # print data
        if data["express_status"] != 3:
            #状态码不等于3则代表没有签收 需要重新入redis 等待下一轮更新,此时需要重置更新时间为当前时间
            update_times = datetime.datetime.now()
            update_times = update_times.strftime('%Y-%m-%d %H:%M:%S')
            again_data = {"id":data['id'],"package_no":data['package_no'],"company_code":data['company_code'],'update_time':update_times,'create_time':create_time}
            #在redis的右侧队列添加新的元素
            red.rpush('datas',again_data)
        if r.status_code == 200:
            # print r.text
            rjson = r.json()
            if rjson['status']:
                logger.info('updated success {0} '.format(id))
            else:
                logger.error('updated {0} failed: {1}'.format(id, rjson))
                err_update(data, create_time)
        else:
            logger.error('updated failed return code: {0}'.format(r.status_code))
            err_update(data, create_time)
    except Exception as e:
        logger.error("update express failed id:" + str(id) + str(e))
        err_update(data, create_time)


def get_need_update_list():
    """
    获取需更新的快递信息
    :return:
    """
    result = ''
    try:
        result = red.lpop('datas')
        # print 'redis中取出数据信息'
        # print result
        result = result.replace("'",'"')
    except Exception as e:
        msg = str.format("{0} : {1}", Exception, str(e))
        logger.warning(msg)
        red.rpush('datas', result)
        # print msg
        return []
    logger.debug('already get need update list')

    list_ = []
    # line = json.loads(result)
    line = eval(result)
    id = int(line['id'])
    package_no = line['package_no'].strip()
    company_code = line['company_code']
    update_time = line['update_time']
    create_time = line['create_time']
    # update_time = datetime.datetime.strptime(line['update_time'], "%Y-%m-%d %H:%M:%S")
    # express_status = line['express_status']
    # transport_status = line['transport_status']

    if not re.match(r'^[\w ]+$', package_no):
        logger.warning('package_no is invalid for id: {0} {1}'.format(id, package_no))
        pass

    line_ = {'id': id, 'package_no': package_no, 'company_code': company_code,'update_time':update_time,'create_time':create_time}
    logger.info('need update id: {0}'.format(id))
    # count += 1
    list_.append(line_)
    return list_


"""
trade_status ---> express_status
transport_company ----> company_code
如果程序崩掉请注意redis链接是否正常，redis内数据是否正常

"""


def main():
    logger.info("main start get need list")
    err_time = 0
    data = ''
    while True:
        try:
            #开始查询一次redis中列表长度，用来控制判断循环的次数
            len_num = red.llen('datas')
            # print 'redis队列中长度'
            # print len_num
            need_num = 0
        except Exception as e:
            msg = str.format("{0} : {1}", Exception, str(e))
            logger.warning(msg)
        while True:
            need_list = get_need_update_list()
            exc = ExpressCompany()
            for line in need_list:
                need_num += 1
                now_time = datetime.datetime.now()
                now_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
                d2 = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')
                create_time = line['create_time']
                d0 = datetime.datetime.strptime(create_time,'%Y-%m-%d %H:%M:%S')
                ctime = (d2-d0).days
                # print '判断创建时间间隔天数'
                if ctime >= 60:
                    #判断从创建到现在是否超过60天， 如果超过为异常数据，丢弃，报警
                    err_time += 1
                    logger.warning("express_info {0} create_time out 60 days id:{1}".format(line['package_no'], line['id']))
                    # print '注意出现异常准备抛弃数据'
                    data += str.format("ERROR:error create_time info for express_info: {0} id:{1} package_no:{2}".format(
                        line['company_code'], line['id'], line['package_no'])) + '<br>'
                    red.lpush('err_create_time_info', line)
                    continue
                update_time = line['update_time']
                d1 = datetime.datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S')
                stime = (d2-d1).seconds
                dtime = (d2-d1).days
                if dtime == 0:
                    if stime <= 7200:
                        # print '当天更新，判断间隔秒数'
                        #判断更新时间是否间隔2个小时，如果小于停止更新，直接入redis。等待下次循环更新
                        logger.info('express_info {0} update_time short 2 hours id:{1}'.format(line['package_no'], line['id']))
                        red.rpush('datas', line)
                        continue

                if len(line['package_no'])<7:
                    #物流号长度小于7的直接跳过
                    continue
                package = PackageItem(line['id'],line['package_no'],line['company_code'])
                try:
                    express_info = exc.get_express_info(package)
                    print express_info
                    if len(express_info['express_detail']) > 0:
                        update_express_info(line['id'],line['package_no'],line['company_code'],line['update_time'],line['create_time'],express_info['express_status'],json.dumps({"data":express_info['express_detail']}, ensure_ascii=False))
                    else:
                        logger.warning("crawler {0} failed id:{1}".format(line['package_no'], line['id']))
                        # print '单号问题'
                        red.rpush('datas', line)
                except Exception as e:
                    logger.error("update {0} failed id:{1}".format(line['package_no'], line['id']))
                    red.rpush('datas', line)
            if need_num >= len_num:
                break
            # print need_num

        # print '等待下轮更新中。。。'
        #2小时更新一次
        subject = u'物流信息爬取更新时发现创建时间大于60天数据'
        recipients = {'xiekunkun': 'kunkun.xie@mfashion.com.cn',
                      'zhuyewei': 'yewei.zhu@mfashion.com.cn',
                      }
        if err_time > 10:
            # print '发送邮件..............'
            err_time = 0
            sendemail(data, recipients, subject)
        time.sleep(7200)

def sendemail(data,recipients,subject):
    msg = MIMEText(data, _subtype='html', _charset='utf-8')
    msg['Subject'] = subject
    msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
    msg['To'] = ', '.join([unicode.format(u'{0} <{1}>', item[0], item[1]) for item in recipients.items()])
    server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
    server.login('buddy@mfashion.com.cn', 'rose123')
    server.sendmail('buddy@mfashion.com.cn', recipients.values(), msg.as_string())
    server.quit()


def test():
    print "start test"
    package = PackageItem(33, '5108002697385', U"FYSD", )

    exc = ExpressCompany()
    express_info = exc.get_express_info(package)
    # print json.dumps(express_info)
    # update_express_info(package.id,package.trade_no,json.dumps({"data":express_info['data'],"transport_state":express_info['transport_state']}))
    # update_express_info(package.id,package.trade_no,"")
    # update_express_info(package.id,package.trade_no,"")

    if len(express_info['express_detail']) > 0:
        print express_info
        update_express_info(package.id, package.package_no,package.company_code,express_info['express_status'], json.dumps({"data":express_info['express_detail']},ensure_ascii=False))
    elif len(express_info['express_detail']) == 0:
        update_express_info(package.id, package.package_no,package.company_code,express_info['express_status'], "")


if __name__ == '__main__':
    main()