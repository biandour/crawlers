# -*- coding: utf-8 -*-

import logging
import aiohttp
from lxml import etree
from datetime import datetime
import re
import asyncio
import traceback
from aiohttp.client_exceptions import ClientError
import json
import random
import aiomysql
import hashlib
import aiofiles

logger = logging.getLogger(__file__)
LOG_FILE = 'test.log'
LOG_FORMAT = '###### %(name)s - %(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, format=LOG_FORMAT, datefmt=DATE_FORMAT)

ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'

HOST = 'your ip'


async def get_page(sess, url):
    try:
        async with sess.get(url, headers={'User-agent': ua}) as resp:
            return resp.status, await resp.text()
    except Exception as e:
        logger.warning(e, exc_info=True, stack_info=True)


def get_html(web_page):
    html = etree.HTML(web_page)
    return html


async def fetch_kxdaili():
    """
    从www.kxdaili.com抓取免费代理
    """
    logger.info('start to fetch proxies from kxdaili')
    proxies = []
    urls = ['http://ip.kxdaili.com/ipList/{}.html'.format(_ + 1) for _ in range(10)]
    async with aiohttp.ClientSession() as sess:
        for url in urls:
            try:
                status, page = await get_page(sess, url)
                if status != 200:
                    continue
                html = get_html(page)
                trs = html.xpath('//table[@class="ui table segment"]/tbody/tr')
                for tr in trs:
                    proxy_item = {
                        'ip': tr.xpath('./td[1]/text()')[0] if tr.xpath('./td[1]/text()') else '',
                        'port': tr.xpath('./td[2]/text()')[0] if tr.xpath('./td[2]/text()') else '',
                        'level': tr.xpath('./td[3]/text()')[0] if tr.xpath('./td[3]/text()') else '',
                        'type': tr.xpath('./td[4]/text()')[0] if tr.xpath('./td[4]/text()') else '',
                        'location': tr.xpath('./td[6]/text()')[0] if tr.xpath('./td[6]/text()') else '',
                        'check_time': '',
                    }
                    check_time = tr.xpath('./td[7]/text()')[0] if tr.xpath('./td[7]/text()') else ''
                    if check_time:
                        ret = re.match(r'^(\d*)分(\d+)秒', check_time)
                        if ret:
                            minute, sec = ret.groups() if ret else (0, 0)
                            proxy_item['check_time'] = int(
                                datetime.timestamp(datetime.now()) - int(minute) * 60 - int(sec))
                    type = tr.xpath('./td[3]/text()')[0] if tr.xpath('./td[3]/text()') else ''
                    if type:
                        type = type.lower().split(',')
                        for _ in type:
                            proxy_item['type'] = _
                            proxies.append(proxy_item)
            except Exception as e:
                logger.warning(e, exc_info=True, stack_info=True)
                logger.warning("fail to fetch from kxdaili")
                continue
    return proxies


async def fetch_xici():
    """
    https://www.xicidaili.com/nn/
    """
    logger.info('start to fetch proxies from kxdaili')
    proxies = []
    urls = ['https://www.xicidaili.com/nn/{}'.format(_ + 1) for _ in range(10)]
    async with aiohttp.ClientSession() as sess:
        for url in urls:
            status, page = await get_page(sess, url)
            if status != 200:
                continue
            html = get_html(page)
            trs = html.xpath('//*[@id="ip_list"]/tr[@class]')
            for tr in trs:
                try:
                    proxy_item = {
                        'ip': tr.xpath('./td[2]/text()')[0] if tr.xpath('./td[2]/text()') else '',
                        'port': tr.xpath('./td[3]/text()')[0] if tr.xpath('./td[3]/text()') else '',
                        'loaction': tr.xpath('./td[4]/a/text()')[0] if tr.xpath('./td[4]/a/text()') else '',
                        'level': tr.xpath('./td[5]/text()')[0] if tr.xpath('./td[5]/text()') else '',
                        'type': tr.xpath('./td[6]/text()')[0] if tr.xpath('./td[6]/text()') else '',
                        'check_time': '',
                    }
                    check_time = tr.xpath('./td[10]/text()')[0] if tr.xpath('./td[10]/text()') else '',
                    if check_time:
                        check_time = int(datetime.timestamp(datetime.strptime(check_time[0], '%y-%m-%d %H:%M')))
                        proxy_item['check_time'] = check_time
                    proxies.append(proxy_item)
                except Exception as e:
                    logger.warning(e, exc_info=True, stack_info=True)
                    logger.warning("fail to fetch from xici")
                    continue
    return proxies


async def fetch_66ip(num=300):
    """    
    http://www.66ip.cn/
    每次打开此链接都能得到一批代理, 速度不保证
    num: 每次提取数量 (<=300)
    proxytype: 0 = http, 1 = https
    """
    logger.info('start to fetch proxies from 66ip')
    proxies = []
    num = num if num <= 300 else 300
    base_url = 'http://www.66ip.cn/nmtq.php?getnum={}&isp=0&anonymoustype=0&start=&ports=&export=&ipaddress=&area=0&proxytype={}&api=66ip'
    async with aiohttp.ClientSession() as sess:
        for type in (0, 1):
            for _ in range(4):
                try:
                    status, page = await get_page(sess, base_url.format(num, type))
                    if status != 200:
                        continue
                    ret = re.findall(r'(\d+\.\d+\.\d+\.\d+:\d+)<br', page)
                    for proxy in ret:
                        ip, port = proxy.split(':')
                        proxies.append({
                            'ip': ip,
                            'port': port,
                            'level': '匿名',
                            'type': 'http' if type == 0 else 'https',
                            'check_time': int(datetime.timestamp(datetime.now())),
                            'location': '',
                        })
                except Exception as e:
                    logger.warning(e, exc_info=True, stack_info=True)
                    logger.warning('fail to fetch from 66ip')
                    continue
    return proxies


async def fetch_ip3366():
    """
    从www.ip3366.net/free/抓取代理，该网站对访问评率控制严格
    """
    proxies = []
    base_url = 'http://www.ip3366.net/free/?stype={}&page={}'
    async with aiohttp.ClientSession() as sess:
        for stype in range(4):
            for _ in range(7):
                url = base_url.format(stype + 1, _ + 1)
                status, page = await get_page(sess, url)
                if status != 200:
                    continue
                html = get_html(page)
                trs = html.xpath('//*[@id="list"]/table/tbody/tr')
                for tr in trs:
                    try:
                        proxy_item = {
                            'ip': tr.xpath('./td[1]/text()')[0] if tr.xpath('./td[1]/text()')[0] else '',
                            'port': tr.xpath('./td[2]/text()')[0] if tr.xpath('./td[2]/text()')[0] else '',
                            'level': tr.xpath('./td[3]/text()')[0] if tr.xpath('./td[3]/text()')[0] else '',
                            'type': tr.xpath('./td[4]/text()')[0] if tr.xpath('./td[4]/text()')[0] else '',
                            'location': tr.xpath('./td[5]/text()')[0] if tr.xpath('./td[5]/text()')[0] else '',
                            'check_time': '',
                        }
                        check_time = tr.xpath('./td[7]/text()')[0] if tr.xpath('./td[7]/text()') else '',
                        if check_time:
                            check_time = int(datetime.timestamp(datetime.strptime(check_time[0], '%Y/%m/%d %H:%M:%S')))
                            proxy_item['check_time'] = check_time
                        proxies.append(proxy_item)
                        # print(proxy_item)
                    except Exception as e:
                        logger.warning(e, exc_info=True, stack_info=True)
                        logger.warning("fail to fetch from ip3366")
                        continue
                await asyncio.sleep(random.random() + 0.3)
    return proxies


async def fetch_data5u():
    """
    从www.data5u.com/free/抓取代理，数量较少
    :return:
    """
    logger.info('start fetching proxies from data5u')
    proxies = []
    urls = [
        'http://www.data5u.com/free/gngn/index.shtml',
        'http://www.data5u.com/free/gnpt/index.shtml',
        'http://www.data5u.com/free/gwgn/index.shtml',
        'http://www.data5u.com/free/gnpt/index.shtml'
    ]
    async with aiohttp.ClientSession() as sess:
        for url in urls:
            try:
                status, page = await get_page(sess, url)
                if status != 200:
                    continue
                html = get_html(page)
                uls = html.xpath('//ul[@class="l2"]')
                for ul in uls:
                    proxies.append({
                        'ip': ul.xpath('./span[1]/li/text()')[0] if ul.xpath('./span[1]/li/text()') else '',
                        'port': ul.xpath('./span[2]/li/text()')[0] if ul.xpath('./span[2]/li/text()') else '',
                        'level': ul.xpath('./span[3]/li/text()')[0] if ul.xpath('./span[3]/li/text()') else '',
                        'type': ul.xpath('./span[4]/li/text()')[0] if ul.xpath('./span[4]/li/text()') else '',
                        'location': ul.xpath('./span[5]/li/text()')[0] if ul.xpath('./span[5]/li/text()') else '',
                        'check_time': ''
                    })
            except Exception as e:
                logger.warning(e, exc_info=True, stack_info=True)
                logger.warning("fail to fetch from data5u")
                continue
    return proxies


async def check(sess, proxy):
    # logger.info("checking proxies validation")
    # conn = aiohttp.TCPConnector(limit=30)
    # async with aiohttp.ClientSession(connector=conn) as sess:
    p_type = proxy.get('type') or ''
    p_type = p_type.lower()
    if p_type == 'https':
        proxy['type'] = 'https'
    else:
        proxy['type'] = 'http'
    url = proxy['type'] + "://www.httpbin.org/ip"
    proxy_str = 'http://{}:{}'.format(proxy['ip'], proxy['port'])
    try:
        print('checking ' + proxy_str)
        async with sess.get(url, headers={'User-Agent': ua}, proxy=proxy_str,
                            allow_redirects=False, timeout=60, verify_ssl=False) as resp:
            content = await resp.json(encoding='utf8')
            origin_list = content['origin'].split(', ')
            if resp.status == 200 and HOST not in origin_list:
                logger.info('######## {} is available'.format(proxy_str))
                return proxy
            else:
                logger.info('{} is not available'.format(proxy_str))
    except Exception as err:
        logger.info(err)
        logger.info(err, exc_info=True, stack_info=True)
        logger.info('{} is not available'.format(proxy_str))


def level2num(level):
    if level.startswith('高匿'):
        return 2
    if level == '匿名' or level == '普匿' or level.startswith('普通'):
        return 1
    if level.startswith('透明'):
        return 0
    return 0


async def write_sql(pool, proxy):
    sha1 = hashlib.sha1()
    sha1.update('{}://{}:{}'.format(proxy['type'], proxy['ip'], proxy['port']).encode('utf8'))
    type = 1 if proxy['type'] == 'https' else 0
    level = level2num(proxy['level'])
    sql = '''insert into `t_crawler_proxies` (`id`,`ip`,`port`,`type`, `level`, `location`) 
            values ('{}','{}','{}',{},{},'{}') ON DUPLICATE KEY UPDATE `update_time` = CURRENT_TIMESTAMP '''.format(
        sha1.hexdigest(), proxy['ip'], proxy['port'], type, level, proxy['location'])
    # print(sql)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(sql)
                await cur.fetchone()
                await conn.commit()
                return proxy
            except Exception as e:
                await conn.rollback()
                await conn.commit()
                logger.warning(e, exc_info=True, stack_info=True)
                logger.warning('insert proxy: {} failed'.format(proxy))


async def write_file(f, proxy):
    item = json.dumps(proxy, ensure_ascii=False)
    await f.write(item + '\n')
    print('write {} to file.'.format(item))


async def check_and_write(proxy, sess, pool, f, sem):
    async with sem:
        try:
            checked = await check(sess, proxy)
            if checked:
                duplicated = await write_sql(pool, checked)
                if duplicated:
                    await write_file(f, duplicated)
        except Exception as e:
            logger.warning(e, exc_info=True, stack_info=True)


async def routine(sem):
    tasks = [
        asyncio.ensure_future(fetch_kxdaili()),
        # asyncio.ensure_future(fetch_xici()),
        asyncio.ensure_future(fetch_66ip()),
        asyncio.ensure_future(fetch_ip3366()),
        asyncio.ensure_future(fetch_data5u()),
    ]
    try:
        results = await asyncio.gather(*tasks)
        new_tasks = []
        logger.info("checking proxies validation and insert into database")
        # conn = aiohttp.TCPConnector(limit=30)
        async with aiohttp.ClientSession() as sess:
            async with aiomysql.create_pool(host='127.0.0.1', port=3306,
                                            user='root', password='password',
                                            db='crawler_data_db') as pool:
                async with aiofiles.open('json_proxies.dat', 'w') as f:
                    for result in results:
                        for proxy in result:
                            new_tasks.append(asyncio.ensure_future(check_and_write(proxy, sess, pool, f, sem)))
                    await asyncio.gather(*new_tasks)
    except Exception as e:
        logger.warning(e, exc_info=True, stack_info=True)


def fetch():
    sem = asyncio.Semaphore(100)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(routine(sem))


if __name__ == '__main__':
    fetch()
