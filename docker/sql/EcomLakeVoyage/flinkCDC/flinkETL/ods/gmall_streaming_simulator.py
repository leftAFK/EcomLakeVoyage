#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gmall 实时数仓 - MySQL 流式变更模拟器
======================================
持续向已灌好数据的 MySQL 执行 INSERT/UPDATE，模拟业务系统日常运行。
Flink CDC 通过 binlog 捕获这些增量变更，驱动实时数仓 ETL。

模拟的变更类型:
  维度变更(SCD): 用户改信息/升级/冻结, 商品改价/上下架, 新增优惠券模板
  业务增量:      新订单, 订单状态流转, 新支付, 新退款, 优惠券领取, 购物车加购

前置: 先运行 gmall_mock_data_generator.py 灌入基础数据
依赖: pip install faker pymysql numpy --break-system-packages

用法:
  python3 gmall_streaming_simulator.py --host 10.0.0.1 -u root -p secret
  python3 gmall_streaming_simulator.py --interval 3 --max-events 100    # 每3秒一批,最多100批
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pymysql
from faker import Faker

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

fake = Faker('zh_CN')

# ============================================================
#  从 gmall_mock_data_generator 复用常量和函数
# ============================================================
from gmall_mock_data_generator import (
    BRANDS, BRAND_CATEGORIES, CATEGORY_PRICES, DEFAULT_PRICE,
    CATEGORY_SPECS, DEFAULT_SPECS, COUPON_TEMPLATES,
    REFUND_REASONS, REFUND_TYPES, REFUND_TYPE_WEIGHTS,
    PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS, USER_LEVELS, USER_LEVEL_WEIGHTS,
    SOURCE_TYPES, SOURCE_TYPE_WEIGHTS, USER_STATUS, USER_STATUS_WEIGHTS,
    ORDER_STATUS_NAMES, PAYMENT_WAY_NAMES,
    generate_spu_name, generate_sku_name, generate_sku_attr,
    random_price, random_stock, random_age, age_range_str,
    wchoice,
)


# ============================================================
#  事件类型定义
# ============================================================
EVENT_TYPES = [
    'new_order',           # 新订单 (含明细+状态履历)
    'order_status_change', # 订单状态流转 (UPDATE order + INSERT status_log)
    'new_payment',         # 新支付 (INSERT payment + UPDATE order)
    'new_refund',          # 新退款 (INSERT refund + UPDATE order)
    'coupon_claim',        # 优惠券领取 (INSERT coupon_use)
    'cart_add',            # 购物车加购 (INSERT cart)
    'user_update',         # 用户信息变更 (UPDATE user_info)
    'sku_price_change',    # 商品改价 (UPDATE sku_info)
    'sku_sale_change',     # 商品上下架 (UPDATE sku_info)
    'new_spu_sku',        # 新增商品 (INSERT spu + sku)
    'new_coupon_template', # 新增优惠券模板 (INSERT coupon_info)
]

# 事件权重 (新订单和状态流转最多)
EVENT_WEIGHTS = [
    25,  # new_order
    20,  # order_status_change
    10,  # new_payment
    5,   # new_refund
    8,   # coupon_claim
    12,  # cart_add
    8,   # user_update
    4,   # sku_price_change
    3,   # sku_sale_change
    3,   # new_spu_sku
    2,   # new_coupon_template
]


# ============================================================
#  工具: 从 MySQL 读取已有 ID 范围
# ============================================================

def fetch_id_pool(conn):
    """从 MySQL 读取各表的 ID 范围, 用于后续随机选择"""
    cur = conn.cursor()
    pool = {}

    # 用户 ID (随机取 200 个)
    cur.execute("SELECT id FROM gmall_base.user_info ORDER BY RAND() LIMIT 200")
    pool['user_ids'] = [r[0] for r in cur.fetchall()]

    # SKU ID + 价格 + 名称
    cur.execute("SELECT id, sku_name, price, img_url, category3_id, sku_attr FROM gmall_base.sku_info WHERE is_sale=1 LIMIT 200")
    rows = cur.fetchall()
    pool['skus'] = [{'id': r[0], 'sku_name': r[1], 'price': r[2], 'img_url': r[3],
                     'category3_id': r[4], 'sku_attr': r[5]} for r in rows]

    # 省 ID
    cur.execute("SELECT id FROM gmall_base.base_region WHERE level=1")
    pool['province_ids'] = [r[0] for r in cur.fetchall()]

    # 品牌 ID + 名称
    cur.execute("SELECT id, brand_name FROM gmall_base.base_brand")
    pool['brands'] = [{'id': r[0], 'name': r[1]} for r in cur.fetchall()]

    # 三级分类 ID + 名称
    cur.execute("SELECT id, category_name FROM gmall_base.base_category WHERE level=3")
    pool['cat3'] = [{'id': r[0], 'name': r[1]} for r in cur.fetchall()]

    # 优惠券模板
    cur.execute("SELECT id, coupon_type, full_amount, reduce_amount FROM gmall_base.coupon_info")
    pool['coupons'] = [{'id': r[0], 'type': r[1], 'full': r[2], 'reduce': r[3]} for r in cur.fetchall()]

    # SPU 最大 ID (用于新增)
    cur.execute("SELECT MAX(id) FROM gmall_base.spu_info")
    pool['max_spu_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_base.sku_info")
    pool['max_sku_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_base.coupon_info")
    pool['max_coupon_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.order_info")
    pool['max_order_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.order_detail")
    pool['max_detail_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.order_status_log")
    pool['max_log_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.payment_info")
    pool['max_payment_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.refund_info")
    pool['max_refund_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.coupon_use")
    pool['max_coupon_use_id'] = cur.fetchone()[0] or 0

    cur.execute("SELECT MAX(id) FROM gmall_business.cart_info")
    pool['max_cart_id'] = cur.fetchone()[0] or 0

    # 待支付/支付中的订单 (用于状态流转)
    cur.execute("SELECT id, order_status, user_id, total_amount, out_trade_no, create_time FROM gmall_business.order_info WHERE order_status IN (1001, 1002, 1003) ORDER BY id DESC LIMIT 100")
    pool['active_orders'] = [{'id': r[0], 'status': r[1], 'user_id': r[2], 'amount': r[3],
                               'out_trade_no': r[4], 'create_time': r[5]} for r in cur.fetchall()]

    # 已完成订单 (用于退款)
    cur.execute("SELECT id, user_id, total_amount FROM gmall_business.order_info WHERE order_status=1005 ORDER BY id DESC LIMIT 50")
    pool['completed_orders'] = [{'id': r[0], 'user_id': r[1], 'amount': r[2]} for r in cur.fetchall()]

    cur.close()
    return pool


# ============================================================
#  事件生成器
# ============================================================

def now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def now_ms():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def exec_event_new_order(conn, pool, stats):
    """新订单: INSERT order_info + order_detail + order_status_log"""
    cur = conn.cursor()
    oid = pool['max_order_id'] + 1
    pool['max_order_id'] = oid
    did = pool['max_detail_id']
    lid = pool['max_log_id']

    user_id = random.choice(pool['user_ids'])
    province_id = random.choice(pool['province_ids'])
    ts = datetime.now()

    # 收货快照
    consignee = fake.name()
    consignee_tel = fake.phone_number()
    addr = f"{fake.province()}{fake.city_name()}{fake.district()}{fake.street_address()}"
    out_trade_no = f"OTN{ts.strftime('%Y%m%d%H%M%S')}{oid:08d}"

    # 1-3 个商品
    num = wchoice([1, 2, 3], [60, 30, 10])
    selected = random.sample(pool['skus'], min(num, len(pool['skus'])))
    source_type = wchoice(SOURCE_TYPES, SOURCE_TYPE_WEIGHTS)

    original_total = Decimal('0.00')
    details = []
    for idx, sku in enumerate(selected):
        qty = wchoice([1, 2, 3, 4], [55, 25, 12, 8])
        line_total = Decimal(str(sku['price'])) * qty
        original_total += line_total
        did += 1
        details.append((did, oid, idx + 1, sku, qty, line_total, source_type, ts))

    # 优惠券
    coupon_reduce = Decimal('0.00')
    coupon_id = None
    if random.random() < 0.25 and pool['coupons']:
        c = random.choice(pool['coupons'])
        if c['type'] == 1 and c['full'] and original_total >= c['full']:
            coupon_reduce = c['reduce']
            coupon_id = c['id']
        elif c['type'] == 2:
            coupon_reduce = c['reduce']
            coupon_id = c['id']

    total_amount = original_total - coupon_reduce

    # 优惠分摊
    detail_rows = []
    for (detail_id, order_id, line_no, sku, qty, line_total, st, create_ts) in details:
        if coupon_reduce > 0:
            ratio = line_total / original_total if original_total > 0 else 0
            split_coupon = (coupon_reduce * ratio).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        else:
            split_coupon = Decimal('0.0000')
        split_total = (line_total - split_coupon).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        detail_rows.append((detail_id, order_id, line_no, sku['id'], sku['sku_name'], sku['img_url'],
                            sku['price'], qty, create_ts, st, None, coupon_id, split_coupon, split_total))

    # 修正最后一行
    if coupon_reduce > 0 and detail_rows:
        total_split = sum(r[12] for r in detail_rows)
        diff = coupon_reduce - total_split
        last = list(detail_rows[-1])
        last[12] = (last[12] + diff).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        last[13] = (last[13] + diff).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        detail_rows[-1] = tuple(last)

    # INSERT order_info
    cur.execute("""
        INSERT INTO gmall_business.order_info
        (id, consignee, consignee_tel, total_amount, order_status, user_id, payment_way,
         delivery_address, order_comment, out_trade_no, trade_body, create_time, operate_time,
         province_id, coupon_reduce_amount, original_total_amount)
        VALUES (%s,%s,%s,%s,1001,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (oid, consignee, consignee_tel,
          total_amount.quantize(Decimal('0.01')), user_id, addr,
          random.choice([None, None, '尽快发货', '工作日送达']),
          out_trade_no, f"{len(details)}件商品", ts, ts,
          province_id,
          coupon_reduce.quantize(Decimal('0.01')),
          original_total.quantize(Decimal('0.01'))))

    # INSERT order_detail
    cur.executemany("""
        INSERT INTO gmall_business.order_detail
        (id, order_id, order_line_no, sku_id, sku_name, img_url, order_price, sku_num,
         create_time, source_type, source_id, split_activity_amount, coupon_id, split_coupon_amount, split_total_amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,0.0000,%s,%s,%s)
    """, detail_rows)

    # INSERT order_status_log (1001 未支付)
    lid += 1
    cur.execute("""
        INSERT INTO gmall_business.order_status_log (id, order_id, order_status, create_time)
        VALUES (%s, %s, 1001, %s)
    """, (lid, oid, ts))

    # 优惠券锁定
    if coupon_id:
        cuid = pool['max_coupon_use_id'] + 1
        pool['max_coupon_use_id'] = cuid
        cur.execute("""
            INSERT INTO gmall_business.coupon_use
            (id, coupon_id, coupon_type, user_id, order_id, coupon_status, coupon_reduce_amount,
             get_time, lock_time, expire_time)
            VALUES (%s,%s,%s,%s,%s,1404,%s,%s,%s,%s)
        """, (cuid, coupon_id, 1 if coupon_reduce > 0 else 2, user_id, oid,
              coupon_reduce.quantize(Decimal('0.01')),
              ts - timedelta(days=random.randint(1, 15)), ts,
              ts + timedelta(days=30)))

    pool['max_detail_id'] = did
    pool['max_log_id'] = lid
    conn.commit()
    cur.close()

    stats['new_order'] += 1
    return f"[{now_str()}] 新订单 #{oid} 用户={user_id} 金额={total_amount} 商品数={len(details)} 优惠={coupon_reduce}"


def exec_event_order_status_change(conn, pool, stats):
    """订单状态流转: UPDATE order_info + INSERT status_log"""
    if not pool['active_orders']:
        return f"[{now_str()}] 无可流转订单,跳过"

    cur = conn.cursor()
    order = random.choice(pool['active_orders'])
    oid = order['id']
    current_status = order['status']
    ts = datetime.now()

    # 状态机: 1001->1002->1003->1005 或 1001->1004(取消) 或 1003->1006->1007(退款)
    transitions = {
        1001: [(1002, 0.6), (1004, 0.15), (1003, 0.25)],  # 未支付 -> 支付中/取消/已支付(快捷)
        1002: [(1003, 0.9), (1004, 0.1)],                   # 支付中 -> 已支付/取消
        1003: [(1005, 0.85), (1006, 0.15)],                 # 已支付 -> 已完成/退款中
    }

    if current_status not in transitions:
        return f"[{now_str()}] 订单 #{oid} 当前状态 {current_status} 无可流转路径,跳过"

    new_status = wchoice(
        [t[0] for t in transitions[current_status]],
        [t[1] for t in transitions[current_status]]
    )

    # UPDATE order_info
    update_fields = "order_status=%s, operate_time=%s, update_time=%s"
    params = [new_status, ts, ts]

    if new_status == 1003:  # 已支付 -> 设支付方式
        pay_way = wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS)
        update_fields += ", payment_way=%s"
        params.append(pay_way)
    elif new_status == 1005:  # 已完成 -> 设收货时间
        update_fields += ", receive_time=%s"
        params.append(ts)

    params.append(oid)
    cur.execute(f"UPDATE gmall_business.order_info SET {update_fields} WHERE id=%s", params)

    # INSERT status_log
    lid = pool['max_log_id'] + 1
    pool['max_log_id'] = lid
    cur.execute("""
        INSERT INTO gmall_business.order_status_log (id, order_id, order_status, create_time)
        VALUES (%s, %s, %s, %s)
    """, (lid, oid, new_status, ts))

    conn.commit()
    cur.close()

    # 更新 pool 中的订单状态
    order['status'] = new_status
    if new_status in (1004, 1005, 1006, 1007):  # 终态/退款中,移出活跃列表
        pool['active_orders'].remove(order)
        if new_status == 1005:
            pool['completed_orders'].append({'id': oid, 'user_id': order['user_id'], 'amount': order['amount']})

    stats['order_status_change'] += 1
    return f"[{now_str()}] 订单 #{oid} 状态流转: {ORDER_STATUS_NAMES.get(current_status,'?')} -> {ORDER_STATUS_NAMES.get(new_status,'?')}"


def exec_event_new_payment(conn, pool, stats):
    """新支付: INSERT payment_info + UPDATE order_info"""
    cur = conn.cursor()
    # 找一个已支付但没支付记录的订单 (status=1003 或 1005)
    cur.execute("""
        SELECT o.id, o.user_id, o.total_amount, o.out_trade_no, o.create_time
        FROM gmall_business.order_info o
        LEFT JOIN gmall_business.payment_info p ON o.id = p.order_id
        WHERE p.id IS NULL AND o.order_status IN (1003, 1005, 1006, 1007)
        ORDER BY o.id DESC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        cur.close()
        return f"[{now_str()}] 无待支付记录的订单,跳过"

    oid, user_id, amount, out_trade_no, order_create = row
    ts = datetime.now()
    pay_type = wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS)
    trade_no = f"TN{ts.strftime('%Y%m%d%H%M%S')}{random.randint(100000, 999999)}"
    pid = pool['max_payment_id'] + 1
    pool['max_payment_id'] = pid

    callback = {
        "trade_status": "TRADE_SUCCESS",
        "total_amount": str(amount),
        "buyer_id": f"2088{random.randint(10**11, 10**12 - 1)}",
        "trade_no": trade_no,
        "notify_time": ts.strftime('%Y-%m-%d %H:%M:%S'),
    }

    cur.execute("""
        INSERT INTO gmall_business.payment_info
        (id, out_trade_no, order_id, user_id, payment_type, trade_no, total_amount,
         payment_status, create_time, callback_time, callback_content)
        VALUES (%s,%s,%s,%s,%s,%s,%s,1002,%s,%s,%s)
    """, (pid, out_trade_no, oid, user_id, pay_type, trade_no, amount, ts, ts,
          json.dumps(callback, ensure_ascii=False)))

    conn.commit()
    cur.close()

    stats['new_payment'] += 1
    return f"[{now_str()}] 新支付 #{pid} 订单={oid} 金额={amount} 方式={PAYMENT_WAY_NAMES.get(pay_type,'?')}"


def exec_event_new_refund(conn, pool, stats):
    """新退款: INSERT refund_info + UPDATE order_info"""
    if not pool['completed_orders']:
        return f"[{now_str()}] 无可退款订单,跳过"

    cur = conn.cursor()
    order = random.choice(pool['completed_orders'])
    oid = order['id']
    user_id = order['user_id']
    amount = order['amount']
    ts = datetime.now()

    # 找该订单的一条明细
    cur.execute("SELECT id, sku_name, split_total_amount, sku_num FROM gmall_business.order_detail WHERE order_id=%s LIMIT 1", (oid,))
    detail = cur.fetchone()
    if not detail:
        cur.close()
        return f"[{now_str()}] 订单 #{oid} 无明细,跳过退款"

    detail_id, sku_name, split_total, sku_num = detail
    refund_amount = split_total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if split_total else amount

    _reason, _reason_type = random.choice(REFUND_REASONS)
    rid = pool['max_refund_id'] + 1
    pool['max_refund_id'] = rid

    cur.execute("""
        INSERT INTO gmall_business.refund_info
        (id, user_id, order_id, order_detail_id, sku_name, refund_amount, refund_num,
         refund_status, refund_type, refund_reason, refund_reason_type, create_time, operate_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,702,%s,%s,%s,%s,%s)
    """, (rid, user_id, oid, detail_id, sku_name, refund_amount, sku_num,
          wchoice(REFUND_TYPES, REFUND_TYPE_WEIGHTS), _reason, _reason_type, ts, ts))

    # UPDATE order_info -> 退款中
    cur.execute("UPDATE gmall_business.order_info SET order_status=1006, operate_time=%s, update_time=%s WHERE id=%s",
                (ts, ts, oid))

    # INSERT status_log
    lid = pool['max_log_id'] + 1
    pool['max_log_id'] = lid
    cur.execute("INSERT INTO gmall_business.order_status_log (id, order_id, order_status, create_time) VALUES (%s,%s,1006,%s)",
                (lid, oid, ts))

    conn.commit()
    cur.close()

    pool['completed_orders'].remove(order)
    stats['new_refund'] += 1
    return f"[{now_str()}] 新退款 #{rid} 订单={oid} 金额={refund_amount} 原因={_reason}"


def exec_event_coupon_claim(conn, pool, stats):
    """优惠券领取: INSERT coupon_use"""
    cur = conn.cursor()
    coupon = random.choice(pool['coupons'])
    user_id = random.choice(pool['user_ids'])
    ts = datetime.now()
    cuid = pool['max_coupon_use_id'] + 1
    pool['max_coupon_use_id'] = cuid

    cur.execute("""
        INSERT INTO gmall_business.coupon_use
        (id, coupon_id, coupon_type, user_id, coupon_status, get_time, expire_time)
        VALUES (%s,%s,%s,%s,1401,%s,%s)
    """, (cuid, coupon['id'], coupon['type'], user_id, ts, ts + timedelta(days=30)))

    conn.commit()
    cur.close()

    stats['coupon_claim'] += 1
    return f"[{now_str()}] 优惠券领取 #{cuid} 用户={user_id} 券模板={coupon['id']}"


def exec_event_cart_add(conn, pool, stats):
    """购物车加购: INSERT cart_info"""
    cur = conn.cursor()
    user_id = random.choice(pool['user_ids'])
    sku = random.choice(pool['skus'])
    qty = wchoice([1, 2, 3], [60, 25, 15])
    ts = datetime.now()
    cid = pool['max_cart_id'] + 1
    pool['max_cart_id'] = cid

    cur.execute("""
        INSERT INTO gmall_business.cart_info
        (id, user_id, sku_id, sku_name, category_id, cart_price, sku_num, img_url, sku_attr, is_checked, create_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (cid, user_id, sku['id'], sku['sku_name'], sku['category3_id'],
          sku['price'], qty, sku['img_url'], sku['sku_attr'], random.choice([1, 1, 0]), ts))

    conn.commit()
    cur.close()

    stats['cart_add'] += 1
    return f"[{now_str()}] 购物车加购 #{cid} 用户={user_id} SKU={sku['id']} 数量={qty}"


def exec_event_user_update(conn, pool, stats):
    """用户信息变更: UPDATE user_info (SCD 维度变更)"""
    cur = conn.cursor()
    user_id = random.choice(pool['user_ids'])
    ts = datetime.now()

    # 随机选择变更字段
    change_type = wchoice(
        ['nick_name', 'phone_num', 'user_level_up', 'status_freeze', 'status_unfreeze', 'email'],
        [25, 20, 15, 10, 10, 20]
    )

    if change_type == 'nick_name':
        new_val = fake.user_name()
        cur.execute("UPDATE gmall_base.user_info SET nick_name=%s, update_time=%s WHERE id=%s", (new_val, ts, user_id))
        desc = f"昵称->{new_val}"
    elif change_type == 'phone_num':
        new_val = fake.phone_number()
        cur.execute("UPDATE gmall_base.user_info SET phone_num=%s, update_time=%s WHERE id=%s", (new_val, ts, user_id))
        desc = f"手机号->{new_val}"
    elif change_type == 'email':
        new_val = fake.email()
        cur.execute("UPDATE gmall_base.user_info SET email=%s, update_time=%s WHERE id=%s", (new_val, ts, user_id))
        desc = f"邮箱->{new_val}"
    elif change_type == 'user_level_up':
        # 查当前等级, 升一级
        cur.execute("SELECT user_level FROM gmall_base.user_info WHERE id=%s", (user_id,))
        row = cur.fetchone()
        if row and row[0] and row[0] < 6:
            new_level = row[0] + 1
            cur.execute("UPDATE gmall_base.user_info SET user_level=%s, update_time=%s WHERE id=%s", (new_level, ts, user_id))
            desc = f"等级升级 Lv{row[0]}->Lv{new_level}"
        else:
            desc = "等级已满,跳过"
    elif change_type == 'status_freeze':
        cur.execute("UPDATE gmall_base.user_info SET status=1002, update_time=%s WHERE id=%s AND status=1001", (ts, user_id))
        desc = "状态->冻结"
    elif change_type == 'status_unfreeze':
        cur.execute("UPDATE gmall_base.user_info SET status=1001, update_time=%s WHERE id=%s AND status=1002", (ts, user_id))
        desc = "状态->正常(解冻)"

    conn.commit()
    cur.close()

    stats['user_update'] += 1
    return f"[{now_str()}] 用户变更 #{user_id} {desc}"


def exec_event_sku_price_change(conn, pool, stats):
    """商品改价: UPDATE sku_info (SCD 维度变更)"""
    cur = conn.cursor()
    sku = random.choice(pool['skus'])
    ts = datetime.now()

    # 价格变动 -10% ~ +15%
    old_price = Decimal(str(sku['price']))
    change_ratio = random.uniform(-0.10, 0.15)
    new_price = (old_price * Decimal(str(1 + change_ratio))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    cur.execute("UPDATE gmall_base.sku_info SET price=%s, update_time=%s WHERE id=%s", (new_price, ts, sku['id']))

    # 同步更新 pool 中的缓存
    sku['price'] = new_price

    conn.commit()
    cur.close()

    stats['sku_price_change'] += 1
    arrow = "+" if change_ratio > 0 else ""
    return f"[{now_str()}] 商品改价 SKU={sku['id']} {old_price}->{new_price} ({arrow}{change_ratio*100:.1f}%)"


def exec_event_sku_sale_change(conn, pool, stats):
    """商品上下架: UPDATE sku_info.is_sale"""
    cur = conn.cursor()
    sku = random.choice(pool['skus'])
    ts = datetime.now()

    # 查当前状态, 取反
    cur.execute("SELECT is_sale FROM gmall_base.sku_info WHERE id=%s", (sku['id'],))
    row = cur.fetchone()
    if not row:
        cur.close()
        return f"[{now_str()}] SKU={sku['id']} 不存在,跳过"

    old_sale = row[0]
    new_sale = 0 if old_sale == 1 else 1
    cur.execute("UPDATE gmall_base.sku_info SET is_sale=%s, update_time=%s WHERE id=%s", (new_sale, ts, sku['id']))

    conn.commit()
    cur.close()

    stats['sku_sale_change'] += 1
    action = "下架" if new_sale == 0 else "上架"
    return f"[{now_str()}] 商品{action} SKU={sku['id']} ({sku['sku_name'][:30]})"


def exec_event_new_spu_sku(conn, pool, stats):
    """新增商品: INSERT spu_info + sku_info"""
    cur = conn.cursor()
    brand = random.choice(pool['brands'])
    brand_cats = BRAND_CATEGORIES.get(brand['name'], [])
    if brand_cats:
        cat3_name = random.choice(brand_cats)
        cat3_entry = next((c for c in pool['cat3'] if c['name'] == cat3_name), None)
        if cat3_entry:
            cat3_id = cat3_entry['id']
        else:
            cat3_id = random.choice(pool['cat3'])['id']
    else:
        cat3 = random.choice(pool['cat3'])
        cat3_name = cat3['name']
        cat3_id = cat3['id']

    spu_name = generate_spu_name(brand['name'], cat3_name)
    ts = datetime.now()
    spu_id = pool['max_spu_id'] + 1
    pool['max_spu_id'] = spu_id

    cur.execute("""
        INSERT INTO gmall_base.spu_info
        (id, spu_name, description, category3_id, brand_id, create_time)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (spu_id, spu_name, f"{brand['name']}正品,{cat3_name},新品上市", cat3_id, brand['id'], ts))

    # 生成 2-3 个 SKU
    specs_def = CATEGORY_SPECS.get(cat3_name, DEFAULT_SPECS)
    spec_combos = []
    if len(specs_def) == 1:
        spec_combos = [{specs_def[0][0]: v} for v in specs_def[0][1]]
    else:
        for v1 in specs_def[0][1]:
            for v2 in specs_def[1][1]:
                spec_combos.append({specs_def[0][0]: v1, specs_def[1][0]: v2})
    random.shuffle(spec_combos)
    num_skus = min(len(spec_combos), random.randint(2, 3))

    for spec_dict in spec_combos[:num_skus]:
        sku_id = pool['max_sku_id'] + 1
        pool['max_sku_id'] = sku_id
        price = random_price(cat3_name)
        sku_name = generate_sku_name(spu_name, spec_dict)

        cur.execute("""
            INSERT INTO gmall_base.sku_info
            (id, sku_name, spu_id, category3_id, brand_id, price, weight, img_url, is_sale, sku_attr, create_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s)
        """, (sku_id, sku_name, spu_id, cat3_id, brand['id'], price,
              Decimal(str(round(random.uniform(0.1, 20.0), 2))),
              f"https://img.gmall.com/sku/{sku_id:06d}.jpg",
              generate_sku_attr(spec_dict), ts))

    conn.commit()
    cur.close()

    stats['new_spu_sku'] += 1
    return f"[{now_str()}] 新增商品 SPU={spu_id} {spu_name} ({num_skus}个SKU)"


def exec_event_new_coupon_template(conn, pool, stats):
    """新增优惠券模板: INSERT coupon_info"""
    cur = conn.cursor()
    ts = datetime.now()
    cid = pool['max_coupon_id'] + 1
    pool['max_coupon_id'] = cid

    # 随机生成一个新优惠券
    ctype = wchoice([1, 2], [70, 30])
    if ctype == 1:
        full = wchoice([99, 199, 299, 499, 999], [20, 25, 25, 20, 10])
        reduce = wchoice([10, 20, 30, 50, 80, 100], [20, 25, 20, 15, 10, 10])
        name = f"限时满{full}减{reduce}券"
        condition = f"满{full}元可用,限时发放"
    else:
        reduce = wchoice([5, 10, 15, 20], [25, 30, 25, 20])
        name = f"无门槛{reduce}元券"
        condition = "无门槛,全场通用"
        full = None

    cur.execute("""
        INSERT INTO gmall_base.coupon_info
        (id, coupon_type, full_amount, reduce_amount, coupon_name, use_condition, create_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (cid, ctype, Decimal(str(full)) if full else None, Decimal(str(reduce)), name, condition, ts))

    # 加入 pool
    pool['coupons'].append({'id': cid, 'type': ctype, 'full': Decimal(str(full)) if full else None, 'reduce': Decimal(str(reduce))})

    conn.commit()
    cur.close()

    stats['new_coupon_template'] += 1
    return f"[{now_str()}] 新增优惠券模板 #{cid} {name}"


# ============================================================
#  事件分发
# ============================================================

EVENT_HANDLERS = {
    'new_order':           exec_event_new_order,
    'order_status_change': exec_event_order_status_change,
    'new_payment':         exec_event_new_payment,
    'new_refund':          exec_event_new_refund,
    'coupon_claim':        exec_event_coupon_claim,
    'cart_add':            exec_event_cart_add,
    'user_update':         exec_event_user_update,
    'sku_price_change':    exec_event_sku_price_change,
    'sku_sale_change':     exec_event_sku_sale_change,
    'new_spu_sku':         exec_event_new_spu_sku,
    'new_coupon_template': exec_event_new_coupon_template,
}


def run_event(conn, pool, stats):
    """随机选择并执行一个事件"""
    event_type = wchoice(EVENT_TYPES, EVENT_WEIGHTS)
    handler = EVENT_HANDLERS[event_type]
    try:
        msg = handler(conn, pool, stats)
        return msg
    except Exception as e:
        conn.rollback()
        return f"[{now_str()}] [ERROR] 事件 {event_type} 执行失败: {e}"


# ============================================================
#  主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='gmall MySQL 流式变更模拟器')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('-P', '--port', type=int, default=3306)
    parser.add_argument('-u', '--user', default='root')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('--interval', type=float, default=2.0, help='每批事件间隔(秒)')
    parser.add_argument('--batch-size', type=int, default=3, help='每批事件数量')
    parser.add_argument('--max-events', type=int, default=0, help='最大批次数(0=无限)')
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
        if HAS_NUMPY:
            np.random.seed(args.seed)

    # 连接 MySQL
    try:
        conn = pymysql.connect(host=args.host, port=args.port, user=args.user,
                               password=args.password, charset='utf8mb4')
        print(f"[OK] 已连接 MySQL: {args.host}:{args.port}")
    except pymysql.Error as e:
        print(f"[ERROR] MySQL 连接失败: {e}")
        sys.exit(1)

    # 读取已有数据
    print("[INFO] 正在读取已有数据...")
    pool = fetch_id_pool(conn)
    print(f"[INFO] 数据池: 用户{len(pool['user_ids'])} SKU{len(pool['skus'])} 品牌{len(pool['brands'])} "
          f"分类{len(pool['cat3'])} 优惠券{len(pool['coupons'])} 活跃订单{len(pool['active_orders'])} "
          f"已完成订单{len(pool['completed_orders'])}")
    print(f"[INFO] ID 计数器: order={pool['max_order_id']} detail={pool['max_detail_id']} "
          f"spu={pool['max_spu_id']} sku={pool['max_sku_id']}")

    # 统计
    stats = {et: 0 for et in EVENT_TYPES}
    batch_count = 0

    print(f"\n[启动] 流式模拟器开始运行 | 间隔={args.interval}s 每批={args.batch_size}事件")
    print("=" * 100)
    print("(Ctrl+C 停止)\n")

    try:
        while True:
            if args.max_events > 0 and batch_count >= args.max_events:
                break

            batch_count += 1
            print(f"--- 第 {batch_count} 批 ---")

            for _ in range(args.batch_size):
                msg = run_event(conn, pool, stats)
                print(f"  {msg}")

            # 定期刷新活跃订单池
            if batch_count % 10 == 0:
                cur = conn.cursor()
                cur.execute("SELECT id, order_status, user_id, total_amount, out_trade_no, create_time FROM gmall_business.order_info WHERE order_status IN (1001, 1002, 1003) ORDER BY id DESC LIMIT 100")
                pool['active_orders'] = [{'id': r[0], 'status': r[1], 'user_id': r[2], 'amount': r[3],
                                          'out_trade_no': r[4], 'create_time': r[5]} for r in cur.fetchall()]
                cur.execute("SELECT id, user_id, total_amount FROM gmall_business.order_info WHERE order_status=1005 ORDER BY id DESC LIMIT 50")
                pool['completed_orders'] = [{'id': r[0], 'user_id': r[1], 'amount': r[2]} for r in cur.fetchall()]
                cur.close()

            # 统计汇总
            total = sum(stats.values())
            if total > 0 and total % 50 == 0:
                print(f"\n[统计] 已执行 {total} 个事件:")
                for et, cnt in stats.items():
                    if cnt > 0:
                        print(f"  {et}: {cnt}")
                print()

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n\n[停止] 模拟器已停止,共执行 {batch_count} 批, {sum(stats.values())} 个事件")
        print("\n[统计汇总]")
        for et, cnt in stats.items():
            if cnt > 0:
                print(f"  {et}: {cnt}")

    conn.close()


if __name__ == '__main__':
    main()
