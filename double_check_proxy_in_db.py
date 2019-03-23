import asyncio
import pymysql
import aiomysql
import aiohttp
import logging

logger = logging.getLogger(__file__)
LOG_FILE = 'test.log'
LOG_FORMAT = '###### %(name)s - %(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, format=LOG_FORMAT, datefmt=DATE_FORMAT)

SEC_IN_DAY = 24*60*60
ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
HOST = 'your ip'


async def fetch_old_proxies(pool, num=100):
    async with pool.acquire() as conn:
    # conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='password', db='crawler_data_db')
        cursor = conn.cursor()
        sql = '''
            select `auto_id`, `ip`, `port`, `type`, `update_time` from `t_crawler_proxies` order by `update_time` limit {};
        '''.format(num)
        try:
            await cursor.execute(sql)
            results = await cursor.fetchall()
            return [result[0] for result in results]
        except Exception as e:
            logger.warning(e, exc_info=True)


async def handle_checked_proxies(pool, ret):
    status = ret['status']
    auto_id = ret['auto_id']
    if status:
        sql = '''
            update `t_crawler_proxies` set `update_time`=current_timestamp where `auto_id`={}
        '''.format(auto_id)
    else:
        sql = '''
            delete from `t_crawler_proxies` where `auto_id`={}
        '''.format(auto_id)
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                await conn.commit()
                return 'OK'
    except Exception as e:
        logger.warning(e, exc_info=True)
        return 'FAILED'


async def check(proxy, pool, sess, sem):
    async with sem:
        auto_id, ip, port, int_type, update_time = proxy
        str_type = 'https' if int_type == 1 else 'http'
        url = '{}://www.httpbin.org/ip'.format(str_type)
        proxy_str = 'http://{}:{}'.format(ip, port)
        try:
            print('checking ' + proxy_str)
            async with sess.get(url, headers={'User-Agent': ua}, proxy=proxy_str,
                                allow_redirects=False, timeout=60, verify_ssl=False) as resp:
                content = await resp.json(encoding='utf8')
                origin_list = content['origin'].split(', ')
                if resp.status == 200 and HOST not in origin_list:
                    logger.info('######## {} is available'.format(proxy_str))
                    ret = {'status': True, 'auto_id': auto_id}
                else:
                    logger.info('{} is not available'.format(proxy_str))
                    ret = {'status': False, 'auto_id': auto_id}
                return await handle_checked_proxies(pool, ret)
        except Exception as err:
            logger.info(err)
            logger.info(err, exc_info=True)
            logger.info('check {} failed'.format(proxy_str))


async def update_db(sem, num=200):
    async with aiohttp.ClientSession() as sess:
        async with aiomysql.create_pool(host='127.0.0.1', port=3306, user='root',
                                        password='password', db='crawler_data_db') as pool:
            old_proxies = await fetch_old_proxies(pool, num)
            tasks = [asyncio.ensure_future(check(proxy, pool, sess, sem)) for proxy in old_proxies]
            await asyncio.wait(tasks)


def double_check(concurrency=50):
    sem = asyncio.Semaphore(concurrency)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_db(sem))


if __name__ == '__main__':
    double_check()
