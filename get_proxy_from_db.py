import pymysql
import logging

try_count = 0
logger = logging.getLogger(__file__)
LOG_FILE = 'test.log'
LOG_FORMAT = '###### %(name)s - %(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, format=LOG_FORMAT, datefmt=DATE_FORMAT)


def read_db(num=300, type='http'):
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='password', db='crawler_data_db')
    cursor = conn.cursor()
    if type == 'http' or type == 'https':
        db_type = 1 if type == 'https' else 0
        sql = '''
            SELECT `ip`, `port`, `type` FROM `t_crawler_proxies` WHERE `type`={} ORDER BY `update_time` LIMIT {};
        '''.format(db_type, num)
    else:
        return 'type error'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def handle_result(result):
    proxies = []
    for proxy in result:
        type = 'https' if proxy[2] == 1 else 'http'
        ip = proxy[0]
        port = proxy[1]
        proxies.append('{}://{}:{}'.format(type, ip, port))
    return proxies


def push_new_proxies(num=300, type='http'):
    try:
        global try_count
        logger.info('push new proxies count {}'.format(try_count))
        try_count += 1
        return handle_result(read_db(num, type))
    except Exception as e:
        logger.warning(e, exc_info=True, stack_info=True)
        if try_count < 5:
            push_new_proxies(num=300, type='http')


if __name__ == '__main__':
    print(push_new_proxies(300, 'https'))
