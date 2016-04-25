# -*- coding: utf-8 -*-

"""抓取知乎问题的爬虫
"""

import os
import sys
import json
import urllib
import time
import random
from pymongo import MongoClient

import requests
# 注意lxml是64还是32位的要和python的位数一致
from lxml import etree
from lxml.html import document_fromstring

# fix: “UnicodeDecodeError: 'ascii' codec can't decode byte”
# 修改默认语言为utf8
reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import get_configs
from logger.mylogger import Logger

log_main = Logger.get_logger(__file__)
ospath = 'D:\\Work\\Python\\courtwenshu\\'


class mongo_cli(object):

    def __init__(self):
        # self.dir_root = os.path.dirname(os.path.abspath(__file__))

        # settings = os.path.join(self.dir_root,'setting.yaml')
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client.zhlaw
        self.proid = self.db.probid

cli = mongo_cli()


class zhihu_lawspider(object):
    """docstring for zhihu_lawspider"""

    def __init__(self):
        self.dir_root = os.path.dirname(os.path.abspath(__file__))

        settings = os.path.join(self.dir_root, 'setting.yaml')
        self.configs = get_configs(settings)

        self.url_homepage = self.configs['URL']['HOMEPAGE']
        self.headers_base = self.configs['HEADERS']['BASE']

        self.url_login = self.configs['URL']['LOGIN']
        self.email = self.configs['AUTH']['EMAIL']
        self.password = self.configs['AUTH']['PASSWORD']
        self.payload_login = {
            'email': self.email,
            'password': self.password,
            'rememberme': 'y',
        }

        self.url_questions = self.configs['URL']['QUESTIONS']
        self.payload_question = self.configs['PAYLOAD']['QUESTION']

        self.url_law_questions = self.configs['URL']['LAW_QUESTION']

        self.url_question_prefix = self.configs['URL']['QUESTION_PREFIX']

        self.timeout_query = self.configs['TIMEOUT']['QUERY']

        self.offset = self.configs['OFFSET']

        self.spider = requests.Session()

    def login(self):
        # 登陆部分
        # xsrf = self._get_xsrf(url=self.url_homepage)
        # self.payload_login['_xsrf'] = xsrf

        # print xsrf
        try:
            self.spider.post(self.url_login,
                             headers=self.headers_base,
                             data=self.payload_login,
                             timeout=self.timeout_query)
        except Exception as e:
            log_main.error('Failed to try to login. Error: {0}'.format(e))
            sys.exit(-1)

        with open(os.path.join(self.dir_root, 'cookiefile')) as f:
            cookie = json.load(f)
        self.spider.cookies.update(cookie)

        if self._test_login():
            log_main.info('Login successfully!')
        else:
            log_main.info('Faile to Login! Exit.')
            sys.exit(-1)

    def run(self):
        # 运行程序
        self.login()
        self.crawl_questions()

    def _get_xsrf(self, url=None):
        try:
            res = self.spider.get(url,
                                  headers=self.headers_base,
                                  timeout=self.timeout_query)
        except Exception as e:
            log_main.error('Failed to fetch {0}. Error: {1}'.format(url, e))
            sys.exit(-1)

        try:
            html_con = etree.HTML(res.text.encode('UTF-8', 'ignore'))
        except Exception as e:
            log_main.error('Fail to form dom tree. Error: {1}'.format(url, e))
            sys.exit(-1)

        node_xsrf = html_con.xpath("//input[@name='_xsrf']")[0]
        xsrf = node_xsrf.xpath("@value")[0]
        log_main.info('xsrf for {0}: {1}'.format(url, xsrf))

        return xsrf

    def crawl_questions(self, start=None, offset=None):
        """抓取知乎问题"""
        # xsrf = self._get_xsrf(url=self.url_questions)
        # self.payload_question['_xsrf'] = xsrf

        for id in range(1, 5000):
            questionpage_id = str(id)
            self.payload_question['start'] = start
            self.payload_question['offset'] = offset
            log_main.info('fetch page: {0}'.format(
                self.url_law_questions + questionpage_id))
            try:
                res = self.spider.get(self.url_law_questions + questionpage_id,
                                      headers=self.headers_base,
                                      #    data=self.payload_question,
                                      timeout=self.timeout_query)
            except Exception as e:
                log_main.error('Fail to fetch the page. Error: {0}.'.format(e))
                sys.exit(-1)

            # print res.encoding
            log_main.info('response right! {0}'.format(
                self.url_law_questions + questionpage_id))
            self._parse_json(res.text)
            rand_num = random.randint(1, 20)
            if rand_num % 7 == 0:
                time.sleep(random.randint(10, 20))
            elif rand_num % 10 == 0:
                time.sleep(random.randint(5, 10))

    def _parse_json(self, string):
        # res_json = json.loads(string,'utf-8')['msg']
        try:
            html = document_fromstring(string)
        except Exception as e:
            log_main.error(e)
            sys.exit(-1)
        # 获得问题答案数目
        answerCount = html.xpath(u"//meta[@itemprop='answerCount']")
        ansList = []
        for ans in answerCount:
            ansList.append(int(ans.attrib['content']))

        # nodeId 表示页面中的第几个问题
        nodeId = 0
        # 增加的问题数目
        add_question = 0
        nodes = html.xpath(u"//a[@class='question_link']")

        for node in nodes:
            # logitem_id = int(node.xpath('@id')[0].split('-')[-1])

            question_title = node.text
            question_url = node.attrib['href']
            question_url = self.url_question_prefix + question_url

            question_id = node.attrib['href'].split('/')[2]
            question_answerCount = ansList[nodeId]
            if question_answerCount >= 1:
                if question_title.strip() != '' and question_url.strip() != '' and question_id.strip() != '':
                    is_question = cli.proid.find_one({"_id": question_id})
                    if is_question == None:
                        add_question += 1
                        cli.proid.insert_one(
                            {'_id': question_id, 'question_title': question_title, 'question_url': question_url})
            nodeId += 1
        log_main.info("增加问题: {0}".format(add_question))
        return 0

    def _test_login(self):
        """测试是否登陆成功.

        Output:
        + 测试成功时返回 True，否则返回 False.
        """
        try:
            res = self.spider.get(self.url_homepage,
                                  headers=self.headers_base,
                                  timeout=self.timeout_query)
        except Exception as e:
            log_main.error('Error when testing login: {0}'.format(e))
            return False

        try:
            html_con = etree.HTML(res.text)
        except Exception as e:
            log_main.error('Failed to set dom tree: {0}'.format(e))
            return False

        node_list_title = html_con.xpath("//div[@id='zh-home-list-title']")

        if node_list_title:
            return True
        else:
            return False

if __name__ == '__main__':
    ZHspider = zhihu_lawspider()
    ZHspider.run()
