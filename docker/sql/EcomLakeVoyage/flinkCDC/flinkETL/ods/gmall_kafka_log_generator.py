# 清理快照rm -rf /Users/ok/bigdata/docker/paimon_warehouse/flink-checkpoints/*
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gmall 实时数仓 - Kafka 日志流生成器
=====================================
直接生成用户行为日志 JSON 推送到 Kafka，不写入 MySQL。
5 种日志类型：启动、页面浏览、动作、曝光、错误。
user_id / item_id 引用 MySQL 里的真实数据，保证下游 JOIN 有意义。

依赖:
  pip install faker kafka-python pymysql numpy --break-system-packages

用法:
  # 基础运行 (先从 MySQL 拉取数据池, 然后持续推 Kafka)
  python3 gmall_kafka_log_generator.py --kafka 127.0.0.1:9092 \
    --mysql-host 127.0.0.1 --mysql-user root --mysql-password 123456

  # 自定义速率和 topic 前缀
  python3 gmall_kafka_log_generator.py --kafka 127.0.0.1:9092 \
    --mysql-host 127.0.0.1 --mysql-user root --mysql-password 123456 \
    --tps 20 --topic-prefix ods_log_

  # 只推 1000 条后停止 (测试用)
  python3 gmall_kafka_log_generator.py --kafka 127.0.0.1:9092 \
    --mysql-host 127.0.0.1 --mysql-user root --mysql-password 123456 \
    --max-messages 1000

  # dry-run (只打印, 不发 Kafka, 不需要 MySQL 也能跑)
  python3 gmall_kafka_log_generator.py --dry-run --max-messages 20
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime
from collections import deque

from faker import Faker

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

fake = Faker('zh_CN')

# ============================================================
#  常量定义
# ============================================================

# 应用信息
APP_IDS = ['gmall_app', 'gmall_h5', 'gmall_mini']
OS_TYPES = ['iOS', 'Android', 'HarmonyOS']
OS_WEIGHTS = [40, 50, 10]
APP_VERSIONS = ['2.5.0', '2.4.1', '2.4.0', '2.3.2', '2.3.0', '2.2.5']
CHANNELS = ['appstore', 'huawei', 'xiaomi', 'oppo', 'vivo', 'yingyongbao', 'official']
CHANNEL_WEIGHTS = [20, 18, 15, 12, 10, 15, 10]

# 页面定义
PAGES = {
    'home':      {'name': '首页',       'next': ['category', 'search', 'product_detail', 'cart', 'mine']},
    'category':  {'name': '分类页',     'next': ['product_list', 'home', 'cart']},
    'search':    {'name': '搜索页',     'next': ['product_list', 'home']},
    'product_list': {'name': '商品列表', 'next': ['product_detail', 'category', 'search']},
    'product_detail': {'name': '商品详情', 'next': ['cart', 'order_confirm', 'product_list', 'home']},
    'cart':      {'name': '购物车',     'next': ['order_confirm', 'product_detail', 'home']},
    'order_confirm': {'name': '订单确认', 'next': ['pay', 'cart', 'product_detail']},
    'pay':       {'name': '支付页',     'next': ['order_detail', 'home']},
    'order_detail': {'name': '订单详情', 'next': ['order_list', 'home']},
    'order_list': {'name': '订单列表',   'next': ['order_detail', 'home']},
    'mine':      {'name': '我的',       'next': ['order_list', 'address', 'coupon', 'setting', 'home']},
    'coupon':    {'name': '优惠券',     'next': ['mine', 'product_list']},
    'setting':   {'name': '设置',       'next': ['mine']},
    'address':   {'name': '收货地址',   'next': ['mine', 'order_confirm']},
}

# 动作类型
ACTION_TYPES = [
    'click', 'add_cart', 'collect', 'share', 'search', 'favor',
    'comment', 'like', 'subscribe', 'view_more'
]
ACTION_WEIGHTS = [25, 15, 10, 8, 12, 8, 5, 6, 3, 8]

# 曝光类型
DISPLAY_TYPES = ['recommend', 'search_result', 'category_list', 'flash_sale', 'banner']
DISPLAY_WEIGHTS = [40, 25, 15, 10, 10]

# 错误类型
ERROR_TYPES = [
    ('1001', '网络异常', 'NetworkError: Connection timeout after 10000ms'),
    ('1002', '服务器错误', 'HTTP 500 Internal Server Error'),
    ('1003', '参数错误', 'Invalid parameter: user_id is required'),
    ('1004', '权限不足', 'Permission denied: need login'),
    ('2001', '支付失败', 'Payment failed: insufficient balance'),
    ('2002', '库存不足', 'Out of stock: sku_id=xxx has 0 stock'),
    ('3001', '页面加载失败', 'Page load failed: resource timeout'),
    ('3002', '图片加载失败', 'Image load error: CDN 503'),
]

# 入口类型
ENTRY_TYPES = ['icon', 'push', 'sms', 'share_link', 'widget', 'deep_link']
ENTRY_WEIGHTS = [60, 15, 5, 8, 7, 5]

# 开屏广告
OPEN_ADS = ['ad001', 'ad002', 'ad003', 'ad004', 'ad005', 'ad006', None, None, None, None]  # 40%概率有广告

# 事件类型权重
EVENT_WEIGHTS = {
    'startup': 8,
    'page_view': 40,
    'action': 25,
    'display': 20,
    'error': 2,
}

# 设备池大小 (模拟活跃设备数)
DEVICE_POOL_SIZE = 500


def wchoice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]


# ============================================================
#  数据池 (从 MySQL 读取真实 ID)
# ============================================================

class DataPool:
    def __init__(self):
        self.user_ids = []
        self.sku_ids = []
        self.devices = []
        self._mid_pool = []

    def load_from_mysql(self, host, port, user, password):
        """从 MySQL 读取真实的 user_id 和 sku_id"""
        if not HAS_PYMYSQL:
            print("[WARN] pymysql 未安装, 使用模拟数据")
            self._generate_mock_pool()
            return

        try:
            conn = pymysql.connect(host=host, port=port, user=user, password=password,
                                   charset='utf8mb4', connect_timeout=10)
            cur = conn.cursor()

            # 用户 ID (取 200 个)
            cur.execute("SELECT id FROM gmall_base.user_info WHERE status=1001 ORDER BY RAND() LIMIT 200")
            self.user_ids = [r[0] for r in cur.fetchall()]

            # SKU ID (取 200 个上架商品)
            cur.execute("SELECT id, sku_name FROM gmall_base.sku_info WHERE is_sale=1 ORDER BY RAND() LIMIT 200")
            self.sku_ids = [r[0] for r in cur.fetchall()]

            cur.close()
            conn.close()

            print(f"[OK] 从 MySQL 加载数据: 用户={len(self.user_ids)} SKU={len(self.sku_ids)}")
        except pymysql.Error as e:
            print(f"[WARN] MySQL 连接失败, 使用模拟数据: {e}")
            self._generate_mock_pool()

    def _generate_mock_pool(self):
        """没有 MySQL 时生成模拟数据"""
        self.user_ids = list(range(1, 501))
        self.sku_ids = list(range(1, 201))
        print(f"[INFO] 使用模拟数据池: 用户={len(self.user_ids)} SKU={len(self.sku_ids)}")

    def init_devices(self, count):
        """初始化设备池"""
        for i in range(count):
            mid = f"M{random.randint(10**9, 10**10 - 1)}"
            self._mid_pool.append(mid)
        print(f"[INFO] 初始化设备池: {count} 台设备")

    def random_user(self):
        """随机选一个用户, 有 10% 概率未登录(返回 None)"""
        if random.random() < 0.1:
            return None
        return random.choice(self.user_ids)

    def random_sku(self, count=1):
        """随机选 count 个 SKU"""
        if count == 1:
            return random.choice(self.sku_ids)
        return random.sample(self.sku_ids, min(count, len(self.sku_ids)))

    def random_device(self):
        """随机选一个设备"""
        return random.choice(self._mid_pool)


# ============================================================
#  日志生成器
# ============================================================

class LogGenerator:
    def __init__(self, pool):
        self.pool = pool
        self._id_counter = 0

    def next_id(self):
        self._id_counter += 1
        return self._id_counter

    def gen_startup(self, mid=None, user_id=None):
        """生成启动日志"""
        mid = mid or self.pool.random_device()
        if user_id is None:
            user_id = self.pool.random_user()

        os_type = wchoice(OS_TYPES, OS_WEIGHTS)
        appid = random.choice(APP_IDS)
        version = random.choice(APP_VERSIONS)
        channel = wchoice(CHANNELS, CHANNEL_WEIGHTS)
        entry = wchoice(ENTRY_TYPES, ENTRY_WEIGHTS)

        # 启动耗时: 多数 300-2000ms, 偶尔更慢
        if HAS_NUMPY:
            loading = int(np.random.exponential(scale=800)) + 200
        else:
            loading = int(random.expovariate(1/800)) + 200
        loading = min(loading, 8000)

        # 开屏广告
        ad_id = random.choice(OPEN_ADS)
        ad_ms = random.randint(1000, 5000) if ad_id else None

        return {
            'id': self.next_id(),
            'mid': mid,
            'user_id': user_id,
            'appid': appid,
            'os': os_type,
            'area': fake.province(),
            'version': version,
            'channel': channel,
            'entry': entry,
            'loading_time': loading,
            'open_ad_id': ad_id,
            'open_ad_ms': ad_ms,
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

    def gen_page_view(self, mid=None, user_id=None, last_page=None):
        """生成页面浏览日志"""
        mid = mid or self.pool.random_device()
        if user_id is None:
            user_id = self.pool.random_user()

        # 根据上一页决定当前页
        if last_page and last_page in PAGES:
            next_pages = PAGES[last_page]['next']
            page_id = random.choice(next_pages)
        else:
            page_id = 'home'

        page_name = PAGES[page_id]['name']

        # 停留时长: 多数 5-60 秒
        if HAS_NUMPY:
            during = int(np.random.exponential(scale=25000)) + 2000
        else:
            during = int(random.expovariate(1/25000)) + 2000
        during = min(during, 300000)  # 最多 5 分钟

        # 来源类型
        source_type = wchoice(['home', 'search', 'category', 'push', 'share', 'recommend'],
                               [25, 15, 20, 10, 8, 22])

        return {
            'id': self.next_id(),
            'mid': mid,
            'user_id': user_id,
            'page_id': page_id,
            'page_name': page_name,
            'last_page_id': last_page,
            'jump_count': random.randint(0, 5),
            'during_time': during,
            'source_type': source_type,
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

    def gen_action(self, mid=None, user_id=None, page_id=None):
        """生成动作日志"""
        mid = mid or self.pool.random_device()
        if user_id is None:
            user_id = self.pool.random_user()

        action_type = wchoice(ACTION_TYPES, ACTION_WEIGHTS)

        # 动作对象
        item_type = wchoice(['sku', 'spu', 'brand', 'category', 'shop', 'content'],
                            [45, 20, 10, 10, 5, 10])
        if item_type == 'sku':
            item_id = str(self.pool.random_sku())
        elif item_type == 'spu':
            item_id = str(random.randint(1, 500))
        elif item_type == 'brand':
            item_id = str(random.randint(1, 38))
        elif item_type == 'category':
            item_id = str(random.randint(1, 97))
        else:
            item_id = str(random.randint(1, 1000))

        # 目标页面
        target_pages = {
            'click': ['product_detail', 'product_list'],
            'add_cart': ['cart'],
            'collect': [None],
            'share': [None],
            'search': ['product_list'],
            'favor': [None],
            'comment': ['order_detail'],
            'like': [None],
            'subscribe': [None],
            'view_more': ['product_list'],
        }
        target_page = random.choice(target_pages.get(action_type, [None]))

        return {
            'id': self.next_id(),
            'mid': mid,
            'user_id': user_id,
            'action_type': action_type,
            'item_type': item_type,
            'item_id': item_id,
            'target_page_id': target_page,
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

    def gen_display(self, mid=None, user_id=None, page_id=None):
        """生成曝光日志"""
        mid = mid or self.pool.random_device()
        if user_id is None:
            user_id = self.pool.random_user()

        page_id = page_id or random.choice(['home', 'product_list', 'search', 'category'])
        display_type = wchoice(DISPLAY_TYPES, DISPLAY_WEIGHTS)

        # 曝光商品数: 6-20 个
        item_count = random.randint(6, 20)
        sku_ids = self.pool.random_sku(item_count)
        item_ids_str = ','.join(str(s) for s in sku_ids)
        item_pos_str = ','.join(str(i + 1) for i in range(item_count))

        return {
            'id': self.next_id(),
            'mid': mid,
            'user_id': user_id,
            'page_id': page_id,
            'display_type': display_type,
            'item_ids': item_ids_str,
            'item_pos': item_pos_str,
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

    def gen_error(self, mid=None, user_id=None):
        """生成错误日志"""
        mid = mid or self.pool.random_device()
        if user_id is None:
            user_id = self.pool.random_user()

        err_code, err_name, err_content = random.choice(ERROR_TYPES)

        return {
            'id': self.next_id(),
            'mid': mid,
            'user_id': user_id,
            'appid': random.choice(APP_IDS),
            'err_code': err_code,
            'err_name': err_name,
            'err_content': err_content,
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

    def gen_session(self):
        """生成一个完整的用户会话 (启动 → 浏览 → 动作 → 浏览 → ...)"""
        mid = self.pool.random_device()
        user_id = self.pool.random_user()
        events = []

        # 1. 启动
        startup = self.gen_startup(mid=mid, user_id=user_id)
        events.append(('startup', startup))

        # 2. 浏览若干页面 (3-12 次)
        page_count = random.randint(3, 12)
        last_page = None
        for _ in range(page_count):
            pv = self.gen_page_view(mid=mid, user_id=user_id, last_page=last_page)
            events.append(('page_view', pv))
            last_page = pv['page_id']

            # 每次浏览可能触发曝光
            if random.random() < 0.6:
                display = self.gen_display(mid=mid, user_id=user_id, page_id=pv['page_id'])
                events.append(('display', display))

            # 每次浏览可能触发动作
            if random.random() < 0.4:
                action = self.gen_action(mid=mid, user_id=user_id, page_id=pv['page_id'])
                events.append(('action', action))

            # 小概率出错
            if random.random() < 0.05:
                error = self.gen_error(mid=mid, user_id=user_id)
                events.append(('error', error))

        return events


# ============================================================
#  Kafka 发送
# ============================================================

def send_to_kafka(producer, topic, event_dict):
    """发送 JSON 到 Kafka"""
    try:
        producer.send(
            topic,
            value=json.dumps(event_dict, ensure_ascii=False).encode('utf-8'),
            key=str(event_dict.get('mid', '')).encode('utf-8')
        )
        return True
    except Exception as e:
        print(f"  [ERROR] Kafka 发送失败: {e}")
        return False


# ============================================================
#  主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='gmall Kafka 日志流生成器')
    parser.add_argument('--kafka', default='127.0.0.1:9092', help='Kafka broker 地址')
    parser.add_argument('--topic-prefix', default='ods_log_', help='Kafka topic 前缀')
    parser.add_argument('--tps', type=int, default=10, help='每秒生成条数')
    parser.add_argument('--max-messages', type=int, default=0, help='最大消息数(0=无限)')
    parser.add_argument('--mysql-host', default='127.0.0.1', help='MySQL 主机')
    parser.add_argument('--mysql-port', type=int, default=3306, help='MySQL 端口')
    parser.add_argument('--mysql-user', default='root', help='MySQL 用户')
    parser.add_argument('--mysql-password', default='', help='MySQL 密码')
    parser.add_argument('--no-mysql', action='store_true', help='不连 MySQL, 用模拟数据')
    parser.add_argument('--device-count', type=int, default=500, help='模拟设备数')
    parser.add_argument('--session-mode', action='store_true', help='按会话生成 (更真实, 同设备连续事件)')
    parser.add_argument('--dry-run', action='store_true', help='只打印不发 Kafka')
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
        if HAS_NUMPY:
            np.random.seed(args.seed)

    # 1. 加载数据池
    print("[INFO] 初始化数据池...")
    pool = DataPool()
    if not args.no_mysql and not args.dry_run:
        pool.load_from_mysql(args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password)
    else:
        pool._generate_mock_pool()
    pool.init_devices(args.device_count)

    gen = LogGenerator(pool)

    # 2. 连接 Kafka
    producer = None
    if not args.dry_run:
        if not HAS_KAFKA:
            print("[ERROR] kafka-python 未安装, 请执行: pip install kafka-python")
            sys.exit(1)
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.kafka,
                acks=1,
                batch_size=16384,
                linger_ms=5,
                compression_type='gzip',
            )
            print(f"[OK] 已连接 Kafka: {args.kafka}")
        except Exception as e:
            print(f"[ERROR] Kafka 连接失败: {e}")
            sys.exit(1)

    # 3. topic 映射
    topics = {
        'startup':   f'{args.topic_prefix}startup',
        'page_view': f'{args.topic_prefix}page_view',
        'action':    f'{args.topic_prefix}action',
        'display':   f'{args.topic_prefix}display',
        'error':     f'{args.topic_prefix}error',
    }
    print(f"[INFO] Topic 映射: {topics}")

    # 4. 生成并发送
    print(f"\n[启动] 日志生成器开始运行 | TPS={args.tps} 模式={'会话' if args.session_mode else '单条'}")
    if args.max_messages > 0:
        print(f"[INFO] 最大消息数: {args.max_messages}")
    print("=" * 100)
    print("(Ctrl+C 停止)\n")

    total = 0
    counts = {k: 0 for k in topics}
    interval = 1.0 / args.tps

    try:
        while True:
            if args.max_messages > 0 and total >= args.max_messages:
                break

            if args.session_mode:
                # 会话模式: 一次生成一个完整会话的所有事件
                events = gen.gen_session()
                for event_type, event_data in events:
                    if args.max_messages > 0 and total >= args.max_messages:
                        break

                    topic = topics[event_type]
                    if args.dry_run:
                        print(f"[{event_type}] {json.dumps(event_data, ensure_ascii=False)[:120]}...")
                    else:
                        send_to_kafka(producer, topic, event_data)

                    counts[event_type] += 1
                    total += 1

                # 控制速率: 每个会话后按事件数 sleep
                time.sleep(interval * len(events))
            else:
                # 单条模式: 每条随机选类型
                event_type = wchoice(list(EVENT_WEIGHTS.keys()), list(EVENT_WEIGHTS.values()))
                if event_type == 'startup':
                    event_data = gen.gen_startup()
                elif event_type == 'page_view':
                    event_data = gen.gen_page_view()
                elif event_type == 'action':
                    event_data = gen.gen_action()
                elif event_type == 'display':
                    event_data = gen.gen_display()
                else:
                    event_data = gen.gen_error()

                topic = topics[event_type]
                if args.dry_run:
                    print(f"[{event_type}] {json.dumps(event_data, ensure_ascii=False)[:120]}...")
                else:
                    send_to_kafka(producer, topic, event_data)

                counts[event_type] += 1
                total += 1
                time.sleep(interval)

            # 每 100 条打印统计
            if total > 0 and total % 100 == 0:
                status = " | ".join(f"{k}={v}" for k, v in counts.items() if v > 0)
                print(f"[统计] 已发送 {total} 条 | {status}")

    except KeyboardInterrupt:
        print(f"\n\n[停止] 生成器已停止, 共生成 {total} 条日志")
        print("\n[统计汇总]")
        for k, v in counts.items():
            if v > 0:
                print(f"  {k}: {v}")

    # 关闭 Kafka
    if producer:
        producer.flush()
        producer.close()
        print("\n[INFO] Kafka 连接已关闭")


if __name__ == '__main__':
    main()
