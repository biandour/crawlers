import pymysql


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
    return handle_result(read_db(num, type))


if __name__ == '__main__':
    print(push_new_proxies())
