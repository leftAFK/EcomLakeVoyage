#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电商实时数仓 - MySQL 维度表/业务表 模拟数据生成器
=====================================================
基于 Faker 生成高逼真模拟数据，支持:
  1. 商品维度表 dim_product  (真实品牌+品名+类目+合理价格区间)
  2. 用户维度表 dim_user     (真实姓名+地址+手机号+注册时间分布)
  3. 订单业务表 dwd_order    (引用真实商品/用户ID, 金额符合业务分布)

依赖安装:
  pip install faker pymysql numpy --break-system-packages

使用方式:
  python mysql_mock_data_generator.py                          # 连接 MySQL 建表+灌数
  python mysql_mock_data_generator.py --dry-run                # 仅预览数据，不写库
  python mysql_mock_data_generator.py --products 200 --users 5000 --orders 20000  # 自定义数据量
  python mysql_mock_data_generator.py --host 10.0.0.1 -P 3307 -u admin -p secret  # 自定义连接
"""

import argparse
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal

import pymysql
from faker import Faker

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("[WARN] numpy 未安装，将使用 random 模块替代正态分布。建议: pip install numpy")


# ============================================================
# 全局配置
# ============================================================
fake = Faker('zh_CN')  # 中文 locale，生成中国真实的姓名、地址、手机号

# 真实电商品牌与类目映射 (品牌 -> 主营类目)
BRAND_CATEGORY = {
    '华为':       ['手机', '平板', '笔记本', '智能穿戴', '耳机'],
    '苹果':       ['手机', '平板', '笔记本', '耳机', '智能穿戴'],
    '小米':       ['手机', '平板', '智能穿戴', '智能家居', '耳机'],
    'OPPO':       ['手机', '耳机', '智能穿戴'],
    'vivo':       ['手机', '耳机', '智能穿戴'],
    '三星':       ['手机', '平板', '耳机', '智能穿戴'],
    '联想':       ['笔记本', '平板', '台式机', '显示器'],
    '戴尔':       ['笔记本', '台式机', '显示器'],
    '索尼':       ['耳机', '相机', '游戏机'],
    '耐克':       ['运动鞋', '运动服', '运动装备'],
    '阿迪达斯':   ['运动鞋', '运动服', '运动装备'],
    '李宁':       ['运动鞋', '运动服'],
    '安踏':       ['运动鞋', '运动服'],
    '优衣库':     ['T恤', '卫衣', '裤装', '外套'],
    'ZARA':       ['连衣裙', '衬衫', '外套', '裤装'],
    '海尔':       ['冰箱', '洗衣机', '空调', '热水器'],
    '美的':       ['冰箱', '洗衣机', '空调', '电饭煲'],
    '格力':       ['空调', '电风扇'],
    '飞利浦':     ['电动牙刷', '剃须刀', '吹风机'],
    '罗技':       ['鼠标', '键盘', '音箱'],
    '雀巢':       ['咖啡', '麦片', '奶粉'],
    '伊利':       ['牛奶', '酸奶', '奶粉'],
    '三只松鼠':   ['坚果', '零食', '糕点'],
    '百草味':     ['坚果', '零食', '肉脯'],
    '茅台':       ['白酒'],
    '五粮液':     ['白酒'],
    '巴黎欧莱雅': ['面霜', '精华', '防晒', '彩妆'],
    '雅诗兰黛':   ['精华', '面霜', '眼霜'],
    '兰蔻':       ['精华', '面霜', '彩妆'],
    'SK-II':      ['精华', '面膜'],
    '宜家':       ['家具', '收纳', '灯具', '家居饰品'],
    '得力':       ['文具', '办公耗材'],
    '晨光':       ['文具', '文具'],
}

# 类目 -> 价格区间 (元)
CATEGORY_PRICE_RANGE = {
    '手机':       (899, 12999),
    '平板':       (1299, 9999),
    '笔记本':     (2999, 29999),
    '台式机':     (2999, 19999),
    '智能穿戴':   (299, 5999),
    '耳机':       (99, 3999),
    '智能家居':   (99, 2999),
    '显示器':     (699, 6999),
    '相机':       (1999, 39999),
    '游戏机':     (1499, 5999),
    '运动鞋':     (159, 2999),
    '运动服':     (99, 1599),
    '运动装备':   (49, 999),
    'T恤':        (39, 599),
    '卫衣':       (99, 899),
    '裤装':       (79, 999),
    '外套':       (159, 3999),
    '连衣裙':     (129, 2999),
    '衬衫':       (99, 1299),
    '冰箱':       (1299, 19999),
    '洗衣机':     (899, 9999),
    '空调':       (1599, 12999),
    '热水器':     (599, 3999),
    '电风扇':     (89, 999),
    '电饭煲':     (99, 1999),
    '电动牙刷':   (99, 1999),
    '剃须刀':     (89, 1999),
    '吹风机':     (79, 2999),
    '鼠标':       (39, 999),
    '键盘':       (79, 1599),
    '音箱':       (99, 4999),
    '咖啡':       (29, 399),
    '麦片':       (19, 199),
    '奶粉':       (89, 599),
    '牛奶':       (39, 199),
    '酸奶':       (19, 99),
    '坚果':       (19, 199),
    '零食':       (9, 99),
    '糕点':       (19, 159),
    '肉脯':       (29, 159),
    '白酒':       (299, 39999),
    '面霜':       (89, 2999),
    '精华':       (159, 3999),
    '防晒':       (69, 599),
    '彩妆':       (49, 999),
    '眼霜':       (159, 2999),
    '面膜':       (39, 599),
    '家具':       (199, 19999),
    '收纳':       (19, 399),
    '灯具':       (49, 1999),
    '家居饰品':   (29, 999),
    '文具':       (5, 99),
    '办公耗材':   (9, 299),
}

# 类目 -> 真实商品名后缀片段 (增加逼真度)
PRODUCT_SUFFIXES = {
    '手机':       ['5G 双卡双待', 'Pro 超清影像', 'Plus 长续航', 'Ultra 旗舰版', '青春版', '臻彩版'],
    '笔记本':     ['酷睿 i7', '锐龙版', '轻薄本', '游戏本', '16G+512G', '32G+1T'],
    '运动鞋':     ['透气跑步鞋', '缓震运动鞋', '轻便休闲鞋', '专业训练鞋', '防滑户外鞋'],
    'T恤':        ['纯棉短袖', '冰丝凉感', '宽松圆领', '印花短袖', '修身V领'],
    '白酒':       ['53度酱香型', '52度浓香型', '收藏级礼盒', '年份原浆', '生肖纪念版'],
    '面霜':       ['保湿修护', '抗皱紧致', '舒缓特润', '亮肤精华', '夜间修护'],
}

# 订单状态流转
ORDER_STATUS = ['待支付', '已支付', '已发货', '已完成', '已取消', '已退款']
ORDER_STATUS_WEIGHTS = [5, 10, 20, 55, 5, 5]  # 已完成最多

# 支付方式
PAYMENT_METHODS = ['微信支付', '支付宝', '银行卡', '信用卡', '花呗', '京东白条']
PAYMENT_WEIGHTS = [35, 35, 10, 8, 7, 5]

# 省份权重 (人口/经济规模加权)
PROVINCE_WEIGHTS = {
    '广东省': 120, '江苏省': 85, '浙江省': 80, '山东省': 100, '河南省': 95,
    '四川省': 90, '湖北省': 60, '湖南省': 70, '河北省': 75, '安徽省': 65,
    '福建省': 45, '北京市': 50, '上海市': 50, '重庆市': 35, '陕西省': 40,
    '江西省': 45, '辽宁省': 42, '黑龙江省': 32, '吉林省': 25, '山西省': 35,
    '云南省': 48, '贵州省': 38, '广西壮族自治区': 50, '甘肃省': 25, '海南省': 15,
    '内蒙古自治区': 24, '新疆维吾尔自治区': 26, '宁夏回族自治区': 8, '青海省': 8, '西藏自治区': 4, '天津市': 30,
}


def weighted_choice(options, weights):
    """加权随机选择"""
    return random.choices(options, weights=weights, k=1)[0]


def random_province():
    """按人口权重随机选择省份"""
    provinces = list(PROVINCE_WEIGHTS.keys())
    weights = list(PROVINCE_WEIGHTS.values())
    return weighted_choice(provinces, weights)


def random_price(category):
    """根据类目价格区间生成价格，加一点偏态分布（低价多、高价少）"""
    low, high = CATEGORY_PRICE_RANGE.get(category, (50, 1000))
    if HAS_NUMPY:
        # 偏态分布: 多数低价，少数高价
        base = np.random.beta(2, 5)  # 偏向低端
        price = low + (high - low) * base
    else:
        price = low + (high - low) * (random.random() ** 1.8)
    # 保留到分
    return round(price, 2)


def random_stock():
    """库存量: 多数中等，少数热销缺货"""
    if HAS_NUMPY:
        stock = int(np.random.gamma(shape=2.0, scale=200))
    else:
        stock = int(random.gauss(400, 250))
    return max(0, min(stock, 5000))


def random_business_time(days_back=365):
    """
    生成符合业务时段特征的时间戳:
    - 工作日订单多于周末
    - 白天 (9:00-23:00) 多于凌晨
    - 晚间高峰 (19:00-22:00)
    """
    now = datetime.now()
    start = now - timedelta(days=days_back)

    # 随机选日期 (工作日权重高)
    random_days = random.randint(0, days_back - 1)
    date = start + timedelta(days=random_days)

    # 业务时段分布: 9-12点(上午), 13-18点(下午), 19-22点(晚间高峰), 其他时段少
    hour_weights = [1,1,1,1,1,1,1,2, 3,5,6,6, 4,7,8,8, 7,8,9,10, 9,6,3,2]
    hour = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return date.replace(hour=hour, minute=minute, second=second)


# ============================================================
# 数据生成函数
# ============================================================

def generate_product(product_id):
    """生成单条商品维度数据"""
    brand = random.choice(list(BRAND_CATEGORY.keys()))
    category = random.choice(BRAND_CATEGORY[brand])
    price = random_price(category)

    # 构造真实感商品名: 品牌 + 类目 + 后缀
    name = f'{brand} {category}'
    suffixes = PRODUCT_SUFFIXES.get(category, [])
    if suffixes:
        name += f' {random.choice(suffixes)}'
    # 加上型号特征
    model_suffix = random.choice([
        f'{random.randint(1,9)}代',
        f'{random.choice(["标准版","尊享版","旗舰版","运动版","经典款"])}',
        f'{"".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ", k=random.choice([1,2])))}{random.randint(1,99)}',
        '',  # 部分无后缀
    ])
    if model_suffix:
        name += f' {model_suffix}'

    return {
        'product_id': product_id,
        'product_name': name.strip(),
        'brand': brand,
        'category': category,
        'price': price,
        'stock': random_stock(),
        'status': weighted_choice([1, 0, 2], weights=[80, 10, 10]),  # 1上架 0下架 2缺货
        'create_time': random_business_time(days_back=720),
    }


def generate_user(user_id):
    """生成单条用户维度数据"""
    gender = random.choice(['男', '女'])
    if gender == '男':
        name = fake.name_male()
    else:
        name = fake.name_female()

    province = random_province()
    # 用 Faker 生成城市级地址
    city = fake.city_name()
    district = fake.district()
    detail = fake.street_address()

    # 年龄: 18-55 偏年轻 (网购主力)
    if HAS_NUMPY:
        age = int(np.clip(np.random.normal(30, 9), 18, 65))
    else:
        age = max(18, min(65, int(random.gauss(30, 9))))

    # 注册时间: 近3年，近期更多
    days_ago = int(random.expovariate(1 / 365))  # 指数分布，近期多
    days_ago = min(days_ago, 1095)
    register_time = datetime.now() - timedelta(days=days_ago)

    # 用户等级
    level = weighted_choice([1, 2, 3, 4, 5], weights=[35, 30, 20, 10, 5])

    return {
        'user_id': user_id,
        'username': fake.user_name(),
        'real_name': name,
        'gender': gender,
        'age': age,
        'phone': fake.phone_number(),
        'email': fake.email(),
        'province': province,
        'city': city,
        'address': f'{province}{city}{district}{detail}',
        'level': level,
        'register_time': register_time,
    }


def generate_order(order_id, user_ids, product_list):
    """
    生成单条订单数据
    user_ids:    可用的 user_id 列表
    product_list: 商品信息列表 [{'product_id':.., 'price':..}, ...]
    """
    product = random.choice(product_list)
    user_id = random.choice(user_ids)

    create_time = random_business_time(days_back=180)

    # 购买数量: 多数1-3件
    quantity = weighted_choice([1, 2, 3, 4, 5, 10], weights=[55, 20, 12, 6, 4, 3])

    # 订单金额 = 商品单价 * 数量 (带一点随机折扣/运费)
    base_amount = float(product['price']) * quantity
    discount = random.uniform(0.85, 1.0)  # 0-15% 折扣
    shipping = random.choice([0, 0, 0, random.uniform(5, 25)])  # 多数免邮
    amount = round(base_amount * discount + shipping, 2)

    status = weighted_choice(ORDER_STATUS, ORDER_STATUS_WEIGHTS)

    # 支付时间: 已支付之后的状态才有
    pay_time = None
    if status in ['已支付', '已发货', '已完成', '已退款']:
        pay_lag_minutes = int(np.random.exponential(scale=30)) if HAS_NUMPY else int(random.expovariate(1/30))
        pay_time = create_time + timedelta(minutes=pay_lag_minutes)
        if pay_time > datetime.now():
            pay_time = datetime.now()

    return {
        'order_id': order_id,
        'order_no': f'ORD{create_time.strftime("%Y%m%d")}{order_id:08d}',
        'user_id': user_id,
        'product_id': product['product_id'],
        'quantity': quantity,
        'amount': amount,
        'status': status,
        'payment_method': weighted_choice(PAYMENT_METHODS, PAYMENT_WEIGHTS) if pay_time else None,
        'create_time': create_time,
        'pay_time': pay_time,
    }


# ============================================================
# 建表 DDL
# ============================================================

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id    BIGINT       NOT NULL COMMENT '商品ID',
        product_name  VARCHAR(200) NOT NULL COMMENT '商品名称',
        brand         VARCHAR(50)  NOT NULL COMMENT '品牌',
        category      VARCHAR(50)  NOT NULL COMMENT '类目',
        price         DECIMAL(10,2) NOT NULL COMMENT '售价',
        stock         INT          NOT NULL DEFAULT 0 COMMENT '库存',
        status        TINYINT      NOT NULL DEFAULT 1 COMMENT '状态: 1上架 0下架 2缺货',
        create_time   DATETIME     NOT NULL COMMENT '创建时间',
        PRIMARY KEY (product_id),
        KEY idx_brand (brand),
        KEY idx_category (category)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品维度表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_user (
        user_id        BIGINT       NOT NULL COMMENT '用户ID',
        username       VARCHAR(50)  NOT NULL COMMENT '用户名',
        real_name      VARCHAR(50)  NOT NULL COMMENT '真实姓名',
        gender         VARCHAR(4)   NOT NULL COMMENT '性别',
        age            INT          NOT NULL COMMENT '年龄',
        phone          VARCHAR(20)  NOT NULL COMMENT '手机号',
        email          VARCHAR(100)          COMMENT '邮箱',
        province       VARCHAR(30)  NOT NULL COMMENT '省份',
        city           VARCHAR(30)  NOT NULL COMMENT '城市',
        address        VARCHAR(200)         COMMENT '详细地址',
        level          TINYINT      NOT NULL DEFAULT 1 COMMENT '等级1-5',
        register_time  DATETIME     NOT NULL COMMENT '注册时间',
        PRIMARY KEY (user_id),
        KEY idx_province (province),
        KEY idx_register_time (register_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户维度表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dwd_order (
        order_id        BIGINT        NOT NULL COMMENT '订单ID',
        order_no        VARCHAR(30)   NOT NULL COMMENT '订单编号',
        user_id         BIGINT        NOT NULL COMMENT '用户ID',
        product_id      BIGINT        NOT NULL COMMENT '商品ID',
        quantity        INT           NOT NULL COMMENT '购买数量',
        amount          DECIMAL(12,2) NOT NULL COMMENT '订单金额',
        status          VARCHAR(10)   NOT NULL COMMENT '订单状态',
        payment_method  VARCHAR(20)            COMMENT '支付方式',
        create_time     DATETIME      NOT NULL COMMENT '下单时间',
        pay_time        DATETIME               COMMENT '支付时间',
        PRIMARY KEY (order_id),
        UNIQUE KEY uk_order_no (order_no),
        KEY idx_user_id (user_id),
        KEY idx_product_id (product_id),
        KEY idx_create_time (create_time),
        KEY idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表(业务事实表)'
    """,
]


# ============================================================
# 数据库操作
# ============================================================

def connect_mysql(host, port, user, password, database):
    """连接 MySQL"""
    try:
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, charset='utf8mb4'
        )
        print(f"[OK] 已连接 MySQL: {host}:{port}/{database}")
        return conn
    except pymysql.Error as e:
        print(f"[ERROR] MySQL 连接失败: {e}")
        sys.exit(1)


def execute_ddl(conn):
    """执行建表语句"""
    cur = conn.cursor()
    for ddl in DDL_STATEMENTS:
        table_name = [line.strip() for line in ddl.split('\n')
                      if 'CREATE TABLE' in line][0].split()[-1].strip('(')
        cur.execute(ddl)
        print(f"  [DDL] 表 {table_name} 就绪")
    conn.commit()
    cur.close()


def batch_insert(conn, table, columns, rows, batch_size=500):
    """批量插入"""
    if not rows:
        return 0
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(columns))
    col_str = ','.join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        batch_data = [tuple(row[c] for c in columns) for row in batch]
        cur.executemany(sql, batch_data)
        conn.commit()
        total += len(batch)
        print(f"  [INSERT] {table}: 已写入 {total}/{len(rows)} 行", end='\r')

    print(f"  [INSERT] {table}: 完成 {total} 行{'':20}")
    cur.close()
    return total


# ============================================================
# 主流程
# ============================================================

def preview_data(products, users, orders):
    """预览生成的数据样本"""
    print("\n" + "=" * 80)
    print("数据预览 (前5条)")
    print("=" * 80)

    print("\n--- dim_product 商品维度表 ---")
    for p in products[:5]:
        print(f"  ID={p['product_id']}  {p['product_name']}  品牌={p['brand']}  "
              f"类目={p['category']}  价格={p['price']}  库存={p['stock']}")

    print("\n--- dim_user 用户维度表 ---")
    for u in users[:5]:
        print(f"  ID={u['user_id']}  {u['real_name']}({u['gender']},{u['age']}岁)  "
              f"{u['province']}  手机={u['phone']}  等级=Lv{u['level']}")

    print("\n--- dwd_order 订单业务表 ---")
    for o in orders[:5]:
        print(f"  {o['order_no']}  用户={o['user_id']}  商品={o['product_id']}  "
              f"数量={o['quantity']}  金额={o['amount']}  状态={o['status']}  "
              f"支付={o['payment_method']}")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description='MySQL 电商模拟数据生成器')
    parser.add_argument('--host', default='localhost', help='MySQL 主机')
    parser.add_argument('-P', '--port', type=int, default=3306, help='MySQL 端口')
    parser.add_argument('-u', '--user', default='root', help='MySQL 用户名')
    parser.add_argument('-p', '--password', default='', help='MySQL 密码')
    parser.add_argument('-d', '--database', default='realtime_dw', help='数据库名')
    parser.add_argument('--products', type=int, default=100, help='商品数量')
    parser.add_argument('--users', type=int, default=2000, help='用户数量')
    parser.add_argument('--orders', type=int, default=10000, help='订单数量')
    parser.add_argument('--seed', type=int, default=None, help='随机种子(可复现)')
    parser.add_argument('--dry-run', action='store_true', help='仅预览不写库')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
        if HAS_NUMPY:
            np.random.seed(args.seed)
        print(f"[INFO] 随机种子已设为 {args.seed}，数据可复现")

    print(f"\n[INFO] 开始生成数据: 商品 {args.products} | 用户 {args.users} | 订单 {args.orders}")
    if args.dry_run:
        print("[INFO] dry-run 模式: 仅预览，不写库\n")

    # ---- 1. 生成商品维度数据 ----
    print("[1/3] 生成商品维度表 dim_product ...")
    products = []
    for i in range(1, args.products + 1):
        products.append(generate_product(i))
    print(f"  完成: {len(products)} 条商品")

    # ---- 2. 生成用户维度数据 ----
    print("[2/3] 生成用户维度表 dim_user ...")
    users = []
    for i in range(1, args.users + 1):
        users.append(generate_user(i))
    print(f"  完成: {len(users)} 条用户")

    # ---- 3. 生成订单业务数据 (关联商品+用户) ----
    print("[3/3] 生成订单业务表 dwd_order ...")
    product_list = [{'product_id': p['product_id'], 'price': p['price']} for p in products]
    user_ids = [u['user_id'] for u in users]
    orders = []
    for i in range(1, args.orders + 1):
        orders.append(generate_order(i, user_ids, product_list))
    print(f"  完成: {len(orders)} 条订单")

    # ---- 预览 ----
    preview_data(products, users, orders)

    # ---- 写入 MySQL ----
    if args.dry_run:
        print("\n[INFO] dry-run 模式结束，未写入数据库。")
        print("[提示] 去掉 --dry-run 参数即可写入 MySQL。")
        return

    print("\n[4/4] 写入 MySQL ...")
    conn = connect_mysql(args.host, args.port, args.user, args.password, args.database)
    execute_ddl(conn)

    batch_insert(conn, 'dim_product',
                 ['product_id', 'product_name', 'brand', 'category', 'price', 'stock', 'status', 'create_time'],
                 products)

    batch_insert(conn, 'dim_user',
                 ['user_id', 'username', 'real_name', 'gender', 'age', 'phone', 'email',
                  'province', 'city', 'address', 'level', 'register_time'],
                 users)

    batch_insert(conn, 'dwd_order',
                 ['order_id', 'order_no', 'user_id', 'product_id', 'quantity', 'amount',
                  'status', 'payment_method', 'create_time', 'pay_time'],
                 orders)

    conn.close()
    print(f"\n[DONE] 全部完成! 共写入 {args.products + args.users + args.orders} 行数据到 {args.database}")


if __name__ == '__main__':
    main()
