#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
电商实时数仓模拟数据生成器
往 MySQL（业务表）和 Kafka（日志）灌入模拟数据

依赖安装：
  pip3 install pymysql kafka-python faker --break-system-packages

用法：
  python3 mock_data_generator.py --mode business  --count 100   # 往 MySQL 灌 100 条订单
  python3 mock_data_generator.py --mode log       --count 50    # 往 Kafka 灌 50 条日志
  python3 mock_data_generator.py --mode all       --count 100   # 两者都灌
  python3 mock_data_generator.py --mode stream    --interval 2  # 持续灌（每 2 秒一批）
============================================================
"""

import argparse
import json
import random
import time
import datetime
from decimal import Decimal

try:
    import pymysql
except ImportError:
    print("请先安装: pip3 install pymysql --break-system-packages")
    exit(1)

try:
    from faker import Faker
except ImportError:
    Faker = None
    print("（可选）安装 faker 可生成更真实的数据: pip3 install faker --break-system-packages")

try:
    from kafka import KafkaProducer
except ImportError:
    print("请先安装: pip3 install kafka-python --break-system-packages")
    exit(1)


# ============================================================
# 配置（根据你的 Docker 环境修改）
# ============================================================
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "gmall_business",          # 你的业务库名
    "charset": "utf8mb4",
}

KAFKA_CONFIG = {
    "bootstrap_servers": ["127.0.0.1:9092"],
    "api_version": (3, 9, 0),
}

KAFKA_TOPICS = {
    "startup":  "ods_log_startup",
    "page_view": "ods_log_page_view",
    "action":   "ods_log_action",
    "display":  "ods_log_display",
    "error":    "ods_log_error",
}

# 模拟数据池
USER_IDS = list(range(1, 101))       # 100 个用户
SKU_IDS = list(range(1, 201))         # 200 个 SKU
SPU_IDS = list(range(1, 51))         # 50 个 SPU
CATEGORY3_IDS = list(range(1, 101))  # 100 个三级分类
CATEGORY2_IDS = list(range(1, 31))   # 30 个二级分类
CATEGORY1_IDS = list(range(1, 11))   # 10 个一级分类
BRAND_IDS = list(range(1, 51))       # 50 个品牌
COUPON_IDS = list(range(1, 21))      # 20 张优惠券
REGION_IDS = list(range(1, 35))      # 34 个省份
PAGE_IDS = ["home", "sku_detail", "cart", "trade", "payment", "activity", "mine", "search"]
ERROR_CODES = ["ERR_001", "ERR_002", "ERR_003", "ERR_004", "ERR_005"]
ERROR_NAMES = {
    "ERR_001": "网络超时",
    "ERR_002": "数据库连接失败",
    "ERR_003": "接口异常",
    "ERR_004": "空指针",
    "ERR_005": "权限不足",
}


# ============================================================
# MySQL 业务数据生成
# ============================================================
def gen_business_data(count):
    """往 MySQL 灌入模拟业务数据：订单、支付、退款、优惠券"""
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    now = datetime.datetime.now()

    # 查询各表当前最大 ID，避免主键冲突
    def get_max_id(table):
        cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}")
        return cursor.fetchone()[0]

    base_order   = get_max_id("order_info")
    base_detail  = get_max_id("order_detail")
    base_payment = get_max_id("payment_info")
    base_refund  = get_max_id("refund_info")
    base_coupon  = get_max_id("coupon_use")

    for i in range(count):
        user_id = random.choice(USER_IDS)
        sku_id = random.choice(SKU_IDS)
        order_id = base_order + i + 1
        order_detail_id = base_detail + i + 1
        total_amount = round(random.uniform(50, 5000), 2)
        original_amount = round(total_amount * random.uniform(1.0, 1.2), 2)
        coupon_reduce = round(original_amount - total_amount, 2)
        sku_num = random.randint(1, 5)
        create_time = now - datetime.timedelta(seconds=random.randint(0, 86400))
        order_price = round(total_amount / sku_num, 2)

        # 1. 插入 order_detail（先）
        sql_detail = """
            INSERT INTO order_detail (id, order_id, order_line_no, sku_id, sku_name,
                img_url, order_price, sku_num, create_time, source_type, source_id,
                split_activity_amount, coupon_id, split_coupon_amount, split_total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_detail, (
            order_detail_id, order_id, 1, sku_id, f"商品_{sku_id}",
            f"https://img.example.com/{sku_id}.jpg", order_price, sku_num,
            create_time, random.choice([1, 2]), random.randint(1, 100),
            round(coupon_reduce * 0.3, 2),
            random.choice(COUPON_IDS) if random.random() < 0.3 else None,
            round(coupon_reduce * 0.7, 2), total_amount
        ))

        # 2. 插入 order_info
        sql_order = """
            INSERT INTO order_info (id, consignee, consignee_tel, total_amount,
                order_status, user_id, payment_way, delivery_address,
                order_comment, out_trade_no, trade_body, create_time,
                expire_time, province_id, coupon_reduce_amount, original_total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_order, (
            order_id, f"收件人_{user_id}", f"138{random.randint(10000000, 99999999)}",
            total_amount, random.choice([1001, 1002, 1003, 1004, 1005]), user_id,
            random.choice([1, 2, 3]), f"地址_{random.randint(1, 100)}",
            "好评", f"trade_{order_id}", f"商品_{sku_id}",
            create_time,
            create_time + datetime.timedelta(hours=1),
            random.choice(REGION_IDS), coupon_reduce, original_amount
        ))

        # 3. 70% 概率插入支付记录
        if random.random() < 0.7:
            sql_payment = """
                INSERT INTO payment_info (id, out_trade_no, order_id, user_id,
                    payment_type, trade_no, payment_status, total_amount, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_payment, (
                base_payment + i + 1, f"trade_{order_id}", order_id, user_id,
                random.choice([1, 2, 3]), f"alipay_{order_id}",
                random.choice([1001, 1002, 1003]), total_amount,
                create_time + datetime.timedelta(minutes=random.randint(1, 30))
            ))

        # 4. 10% 概率插入退款记录
        if random.random() < 0.1:
            refund_amount = round(total_amount * random.uniform(0.5, 1.0), 2)
            sql_refund = """
                INSERT INTO refund_info (id, user_id, order_id, order_detail_id, sku_name,
                    refund_amount, refund_num, refund_status, refund_type,
                    refund_reason, refund_reason_type, create_time, refund_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_refund, (
                base_refund + i + 1, user_id, order_id, order_detail_id, f"商品_{sku_id}",
                refund_amount, sku_num, random.choice([701, 702, 703]),
                random.choice([1, 2]), "用户申请退款", random.choice([1, 2, 3]),
                create_time + datetime.timedelta(hours=random.randint(1, 12)),
                create_time + datetime.timedelta(hours=random.randint(2, 24))
            ))

        # 5. 30% 概率插入优惠券领用
        if random.random() < 0.3:
            coupon_id = random.choice(COUPON_IDS)
            sql_coupon = """
                INSERT INTO coupon_use (id, coupon_id, coupon_type, user_id, order_id,
                    coupon_status, coupon_reduce_amount, get_time, using_time, used_time, expire_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_coupon, (
                base_coupon + i + 1, coupon_id, random.choice([1, 2, 3]), user_id, order_id,
                random.choice([1, 2, 3, 4]), coupon_reduce,
                create_time - datetime.timedelta(days=1),
                create_time if random.random() < 0.5 else None,
                create_time if random.random() < 0.3 else None,
                create_time + datetime.timedelta(days=7)
            ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[MySQL] 成功灌入 {count} 条业务数据（订单+明细+支付+退款+优惠券）")


# ============================================================
# Kafka 日志数据生成
# ============================================================
def gen_log_data(count):
    """往 Kafka 灌入模拟日志数据：启动/浏览/动作/曝光/错误"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        api_version=KAFKA_CONFIG["api_version"],
    )

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    stats = {"startup": 0, "page_view": 0, "action": 0, "display": 0, "error": 0}

    for i in range(count):
        mid = f"mid_{random.randint(1, 500)}"
        user_id = random.choice([u for u in USER_IDS if random.random() < 0.8] + [None])
        log_type = random.choices(
            ["startup", "page_view", "action", "display", "error"],
            weights=[15, 40, 25, 15, 5],
        )[0]

        if log_type == "startup":
            log = {
                "mid": mid,
                "user_id": user_id,
                "appid": "gmall",
                "os": random.choice(["ios", "android"]),
                "area": f"地区_{random.randint(1, 35)}",
                "version": "1.0.0",
                "channel": random.choice(["appstore", "huawei", "xiaomi", "oppo"]),
                "entry": random.choice(["1", "2", "3"]),
                "loading_time": random.randint(500, 3000),
                "open_ad_id": str(random.randint(1, 10)),
                "open_ad_ms": random.randint(1000, 5000),
                "open_ad_skip_sec": random.randint(0, 5),
                "actions": json.dumps([]),
                "displays": json.dumps([]),
                "page_view": json.dumps({}),
                "err": json.dumps({}),
                "ts": int(now.timestamp() * 1000),
                "create_time": now_str,
            }
            topic = KAFKA_TOPICS["startup"]

        elif log_type == "page_view":
            page_id = random.choice(PAGE_IDS)
            last_page = random.choice(PAGE_IDS + ["", ""])
            log = {
                "mid": mid,
                "user_id": user_id,
                "page_id": page_id,
                "page_name": f"页面_{page_id}",
                "last_page_id": last_page,
                "jump_count": random.randint(0, 1) if last_page else 1,
                "during_time": random.randint(1000, 60000),
                "source_type": random.choice(["1", "2", "3"]),
                "err": json.dumps({}),
                "ts": int(now.timestamp() * 1000),
                "create_time": now_str,
            }
            topic = KAFKA_TOPICS["page_view"]

        elif log_type == "action":
            item_type = random.choice(["sku", "spu", "activity"])
            item_id = random.choice(SKU_IDS if item_type == "sku" else SPU_IDS)
            log = {
                "mid": mid,
                "user_id": user_id,
                "page_id": random.choice(PAGE_IDS),
                "page_name": f"页面_{random.choice(PAGE_IDS)}",
                "item_type": item_type,
                "item_id": str(item_id),
                "item_ids": str(item_id),
                "item_pos": str(random.randint(1, 10)),
                "action_type": random.choice(["display_type", "click", "add_cart", "buy"]),
                "err": json.dumps({}),
                "ts": int(now.timestamp() * 1000),
                "create_time": now_str,
            }
            topic = KAFKA_TOPICS["action"]

        elif log_type == "display":
            num_items = random.randint(1, 5)
            item_ids = ",".join(str(random.choice(SKU_IDS)) for _ in range(num_items))
            log = {
                "mid": mid,
                "user_id": user_id,
                "page_id": random.choice(PAGE_IDS),
                "page_name": f"页面_{random.choice(PAGE_IDS)}",
                "display_type": random.choice(["sku", "spu", "activity", "promotion"]),
                "item_ids": item_ids,
                "item_pos": ",".join(str(random.randint(1, 10)) for _ in range(num_items)),
                "err": json.dumps({}),
                "ts": int(now.timestamp() * 1000),
                "create_time": now_str,
            }
            topic = KAFKA_TOPICS["display"]

        else:  # error
            err_code = random.choice(ERROR_CODES)
            log = {
                "mid": mid,
                "user_id": user_id,
                "page_id": random.choice(PAGE_IDS),
                "page_name": f"页面_{random.choice(PAGE_IDS)}",
                "err_code": err_code,
                "err_name": ERROR_NAMES[err_code],
                "err_msg": f"错误详情_{err_code}",
                "ts": int(now.timestamp() * 1000),
                "create_time": now_str,
            }
            topic = KAFKA_TOPICS["error"]

        producer.send(topic, value=log)
        stats[log_type] += 1

    producer.flush()
    producer.close()
    print(f"[Kafka] 成功灌入 {count} 条日志数据: " +
          " | ".join(f"{k}={v}" for k, v in stats.items()))


# ============================================================
# 持续灌数模式
# ============================================================
def gen_stream_data(interval):
    """持续灌数模式：每隔 interval 秒灌一批数据"""
    batch = 0
    while True:
        batch += 1
        count = random.randint(5, 20)
        try:
            gen_business_data(count)
            gen_log_data(count)
            print(f"[Batch {batch}] {datetime.datetime.now().strftime('%H:%M:%S')} - 灌入 {count} 条")
        except Exception as e:
            print(f"[Batch {batch}] 错误: {e}")
        time.sleep(interval)


# ============================================================
# Doris DWS 直接灌数（绕过 Flink，用于快速测试 Grafana）
# ============================================================
def gen_dws_direct_data(count):
    """直接往 Doris DWS 表灌数据，不依赖 Flink 链路。
    生成 7 天历史数据 + 今天的数据，让趋势图也有内容。"""
    try:
        conn = pymysql.connect(
            host="127.0.0.1", port=9030, user="root", password="",
            database="dws", charset="utf8mb4",
        )
    except Exception as e:
        print(f"连接 Doris 失败: {e}")
        print("请确认 Doris FE 在 127.0.0.1:9030 可访问")
        return

    cursor = conn.cursor()
    now = datetime.datetime.now()
    today = now.date()

    # 生成 7 天的日期列表（含今天）
    dates = [today - datetime.timedelta(days=d) for d in range(6, -1, -1)]

    total_inserted = 0

    for dt in dates:
        # ========== 1. dws.trade_user_stats ==========
        for i in range(count):
            user_id = random.choice(USER_IDS)
            order_count = random.randint(1, 20)
            order_total = round(random.uniform(100, 10000), 2)
            payment_total = round(order_total * random.uniform(0.6, 0.9), 2)
            refund_total = round(order_total * random.uniform(0, 0.1), 2)

            sql = """
                INSERT INTO dws.trade_user_stats (
                    user_id, dt, user_nick_name, user_level, age_range, gender,
                    order_count, order_total_amount, order_original_amount,
                    order_coupon_reduce, payment_count, payment_total_amount,
                    refund_count, refund_total_amount, refund_num,
                    coupon_count, coupon_reduce_amount
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                user_id, dt, f"用户_{user_id}", random.randint(1, 3),
                random.choice(["0-18", "19-25", "26-35", "36-50", "50+"]),
                random.randint(0, 1),
                order_count, order_total, round(order_total * 1.15, 2),
                round(order_total * 0.15, 2),
                int(order_count * 0.7), payment_total,
                int(order_count * 0.1), refund_total, int(order_count * 0.1),
                int(order_count * 0.3), round(order_total * 0.1, 2),
            ))
            total_inserted += 1

        # ========== 2. dws.trade_sku_stats ==========
        for i in range(count):
            sku_id = random.choice(SKU_IDS)
            cat1 = random.choice(CATEGORY1_IDS)
            cat2 = random.choice(CATEGORY2_IDS)
            cat3 = random.choice(CATEGORY3_IDS)
            brand_id = random.choice(BRAND_IDS)
            order_count = random.randint(1, 50)
            sku_num = random.randint(1, 100)
            total_amount = round(random.uniform(200, 20000), 2)

            sql = """
                INSERT INTO dws.trade_sku_stats (
                    sku_id, dt, sku_name, spu_id, spu_name,
                    category3_id, category3_name, category2_id, category2_name,
                    category1_id, category1_name, brand_id, brand_name,
                    order_count, order_sku_num, order_total_amount,
                    order_coupon_reduce, order_activity_reduce,
                    cart_count, cart_sku_num
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                sku_id, dt, f"商品_{sku_id}", sku_id // 4 + 1, f"SPU_{sku_id // 4 + 1}",
                cat3, f"三级分类_{cat3}", cat2, f"二级分类_{cat2}",
                cat1, f"一级分类_{cat1}", brand_id, f"品牌_{brand_id}",
                order_count, sku_num, total_amount,
                round(total_amount * 0.1, 2), round(total_amount * 0.05, 2),
                random.randint(1, 30), random.randint(1, 50),
            ))
            total_inserted += 1

        # ========== 3. dws.log_page_stats ==========
        for page_id in PAGE_IDS:
            sql = """
                INSERT INTO dws.log_page_stats (
                    page_id, dt, page_name, pv, uv,
                    total_during_time, total_jump_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                page_id, dt, f"页面_{page_id}",
                random.randint(100, 5000), random.randint(50, 500),
                random.randint(100000, 500000), random.randint(10, 200),
            ))
            total_inserted += 1

        # ========== 4. dws.log_user_action_stats ==========
        for i in range(min(count, 20)):
            user_id = random.choice(USER_IDS)
            sql = """
                INSERT INTO dws.log_user_action_stats (
                    user_id, dt, user_nick_name, user_level,
                    startup_count, page_view_count, action_count,
                    exposure_count, error_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                user_id, dt, f"用户_{user_id}", random.randint(1, 3),
                random.randint(1, 10), random.randint(5, 50),
                random.randint(3, 30), random.randint(2, 20), random.randint(0, 3),
            ))
            total_inserted += 1

        # ========== 5. dws.log_sku_exposure_stats ==========
        for i in range(min(count, 20)):
            sku_id = random.choice(SKU_IDS)
            cat1 = random.choice(CATEGORY1_IDS)
            cat2 = random.choice(CATEGORY2_IDS)
            cat3 = random.choice(CATEGORY3_IDS)
            brand_id = random.choice(BRAND_IDS)
            sql = """
                INSERT INTO dws.log_sku_exposure_stats (
                    sku_id, dt, sku_name, spu_name,
                    category3_name, category2_name, category1_name, brand_name,
                    exposure_count, exposure_user_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                sku_id, dt, f"商品_{sku_id}", f"SPU_{sku_id // 4 + 1}",
                f"三级分类_{cat3}", f"二级分类_{cat2}", f"一级分类_{cat1}", f"品牌_{brand_id}",
                random.randint(50, 5000), random.randint(20, 500),
            ))
            total_inserted += 1

    conn.commit()

    # ========== 验证数据 ==========
    verify_sqls = [
        ("dws.trade_user_stats",        "SELECT COUNT(*) FROM dws.trade_user_stats"),
        ("dws.trade_sku_stats",         "SELECT COUNT(*) FROM dws.trade_sku_stats"),
        ("dws.log_page_stats",          "SELECT COUNT(*) FROM dws.log_page_stats"),
        ("dws.log_user_action_stats",   "SELECT COUNT(*) FROM dws.log_user_action_stats"),
        ("dws.log_sku_exposure_stats",  "SELECT COUNT(*) FROM dws.log_sku_exposure_stats"),
    ]
    print("[Doris DWS] 数据验证:")
    for name, sql in verify_sqls:
        cursor.execute(sql)
        cnt = cursor.fetchone()[0]
        print(f"  {name:35s} {cnt} 行")

    cursor.close()
    conn.close()
    print(f"[Doris DWS] 成功灌入 {total_inserted} 条 DWS 数据（7 天 × {count} 条/天，绕过 Flink）")


# ============================================================
# 主函数
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="电商实时数仓模拟数据生成器")
    parser.add_argument("--mode", choices=["business", "log", "all", "stream", "dws"],
                        default="all", help="生成模式")
    parser.add_argument("--count", type=int, default=100, help="每批数据条数")
    parser.add_argument("--interval", type=int, default=2, help="stream 模式间隔秒数")
    args = parser.parse_args()

    print("=" * 50)
    print("  电商实时数仓模拟数据生成器")
    print(f"  模式: {args.mode} | 数量: {args.count}")
    print("=" * 50)

    if args.mode == "business":
        gen_business_data(args.count)
    elif args.mode == "log":
        gen_log_data(args.count)
    elif args.mode == "all":
        gen_business_data(args.count)
        gen_log_data(args.count)
    elif args.mode == "stream":
        gen_stream_data(args.interval)
    elif args.mode == "dws":
        gen_dws_direct_data(args.count)

    print("\n完成！打开 Grafana http://localhost:3000 查看看板数据。")
