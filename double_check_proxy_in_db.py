import asyncio
import aiomysql
import aiohttp
import logging
import config
import time

logger = logging.getLogger(__file__)
LOG_FILE = config.LOG_FILE
LOG_FORMAT = '###### %(name)s - %(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, format=LOG_FORMAT, datefmt=DATE_FORMAT)

SEC_IN_DAY = 24*60*60
ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
HOST = config.IP
UNAVAILABLE_ERRORS = (asyncio.TimeoutError, aiohttp.ClientProxyConnectionError, aiohttp.ContentTypeError,
                      ConnectionRefusedError)


async def fetch_old_proxies(pool, now, num=50):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            sql = '''
                select `auto_id`, `ip`, `port`, `type`, `update_time` from `t_crawler_proxies`
                where `update_time`< '{}' order by `update_time` limit {};
            '''.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now-SEC_IN_DAY)), num)
            try:
                await cursor.execute(sql)
                results = await cursor.fetchall()
                # print(results)
                return results
            except Exception as e:
                logger.warning(e, exc_info=True)
                return ()


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
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                print(sql)
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
        # ret = {}
        try:
            print(proxy)
            print('checking ' + proxy_str)
            print('checking: ' + url)
            async with sess.get(url, headers={'User-Agent': ua}, proxy=proxy_str,
                                allow_redirects=False, timeout=30, verify_ssl=False) as resp:
                content = await resp.json(encoding='utf8')
                origin_list = content['origin'].split(', ')
                if resp.status == 200 and HOST not in origin_list:
                    logger.info('######## {} is available'.format(proxy_str))
                    print('######## {} is available'.format(proxy_str))
                    ret = {'status': True, 'auto_id': auto_id}
                else:
                    logger.info('####### {} is not available'.format(proxy_str))
                    print('####### {} is not available'.format(proxy_str))
                    ret = {'status': False, 'auto_id': auto_id}
                await handle_checked_proxies(pool, ret)
        except Exception as err:
            if isinstance(err, UNAVAILABLE_ERRORS):
                logger.info(err)
                logger.info(err, exc_info=True)
                logger.info('####### {} is not available'.format(proxy_str))
                print('####### {} is not available'.format(proxy_str))
                ret = {'status': False, 'auto_id': auto_id}
                await handle_checked_proxies(pool, ret)
            else:
                logger.info(err)
                logger.info(err, exc_info=True)
                logger.info('check {} failed'.format(proxy_str))


async def update_db(now, sem, num=1000):
    async with aiohttp.ClientSession() as sess:
        async with aiomysql.create_pool(host=config.HOST, port=3306, user=config.USER,
                                        password=config.PASSWORD, db='crawler_data_db') as pool:
            old_proxies = await fetch_old_proxies(pool, now, num)
            tasks = [asyncio.ensure_future(check(proxy, pool, sess, sem)) for proxy in old_proxies]
            await asyncio.wait(tasks)
            logger.info('------------ this round is done')


def double_check(now, concurrency=50):
    sem = asyncio.Semaphore(concurrency)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_db(now, sem))


if __name__ == '__main__':
    now_time = time.time()
    double_check(now_time)
