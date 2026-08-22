#!/usr/bin/env python3
import json
import os

BASE_DIR = "/Users/ok/bigdata/docker/monitoring/grafana/dashboards"

def count_replace_in_sql(rawSql, old_substr, new_substr):
    count = rawSql.count(old_substr)
    return count, rawSql.replace(old_substr, new_substr)

def process_file(filepath, replacements):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    total_rawsql_replaced = 0
    panels_replaced = 0

    for panel in data.get("panels", []):
        panel_has_replacement = False
        for target in panel.get("targets", []):
            if "rawSql" in target:
                original = target["rawSql"]
                current = original
                panel_count = 0
                for old_sub, new_sub in replacements:
                    cnt, current = count_replace_in_sql(current, old_sub, new_sub)
                    panel_count += cnt
                if panel_count > 0:
                    target["rawSql"] = current
                    total_rawsql_replaced += panel_count
                    panel_has_replacement = True
        if panel_has_replacement:
            panels_replaced += 1

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return panels_replaced, total_rawsql_replaced

def main():
    file1 = os.path.join(BASE_DIR, "01-trading-overview.json")
    file2 = os.path.join(BASE_DIR, "02-product-analysis.json")
    file3 = os.path.join(BASE_DIR, "03-traffic-funnel.json")
    file4 = os.path.join(BASE_DIR, "04-user-region.json")

    reps1 = [
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.gmv_realtime_dashboard WHERE total_gmv > 0 OR total_payment_amount > 0 OR total_order_count > 0 OR total_refund_amount > 0 OR total_coupon_reduce_amount > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.gmv_realtime_dashboard WHERE total_gmv > 0 OR total_order_count > 0)"
        ),
    ]

    reps2 = [
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0 OR cart_count > 0 OR order_count > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0)"
        ),
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.category1_sales_summary WHERE order_total_amount > 0 OR cart_count > 0 OR order_count > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.category1_sales_summary WHERE order_total_amount > 0)"
        ),
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.sku_sales_ranking WHERE order_total_amount > 0 OR cart_count > 0 OR order_sku_num > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.sku_sales_ranking WHERE order_total_amount > 0)"
        ),
    ]

    reps3 = [
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.log_traffic_overview WHERE total_pv > 0 OR total_uv > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.log_traffic_overview WHERE total_pv > 0)"
        ),
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.log_funnel_analysis WHERE browse_users > 0 OR action_users > 0 OR cart_users > 0 OR order_users > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.log_funnel_analysis WHERE browse_users > 0)"
        ),
    ]

    reps4 = [
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.user_trade_ranking WHERE order_total_amount > 0 OR payment_total_amount > 0 OR refund_total_amount > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.user_trade_ranking WHERE order_total_amount > 0)"
        ),
        (
            "WHERE dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0 OR cart_count > 0 OR order_count > 0)",
            "WHERE dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0)"
        ),
        (
            "WHERE b.dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0 OR cart_count > 0 OR order_count > 0)",
            "WHERE b.dt = (SELECT MAX(dt) FROM ads.brand_sales_summary WHERE order_total_amount > 0)"
        ),
    ]

    results = []
    for name, path, reps in [
        ("01-trading-overview.json", file1, reps1),
        ("02-product-analysis.json", file2, reps2),
        ("03-traffic-funnel.json", file3, reps3),
        ("04-user-region.json", file4, reps4),
    ]:
        p, r = process_file(path, reps)
        results.append((name, p, r))
        print(f"{name}: 替换 {p} 个 panel, {r} 条 rawSql 子串")

    print("\n=== 汇总 ===")
    for name, p, r in results:
        print(f"{name}: 替换 panel={p}, rawSql 子串数={r}")

if __name__ == "__main__":
    main()
