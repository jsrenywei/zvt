# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvdata import IntervalLevel
from zvt import init_log
from zvt.recorders.joinquant.quotes.jq_stock_kdata_recorder import JqChinaStockKdataRecorder

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


# 名称	dataschema	provider	comments	download
# 个股资料	Stock	eastmoney,sina	个股和板块为多对多的关系
# 板块资料	Index	eastmoney,sina	板块有行业,概念,区域三个分类的维度,不同的provider分类会有所不同,个股和板块为多对多的关系
# 个股行情	Stock{level}Kdata	joinquant,netease,eastmoney	支持1,5,15,30,60分钟, 日线,周线级别
# 指数日线行情	Index1DKdata	eastmoney,sina,joinquant	指数本质上也是一种板块,指数对应板块资料中的标的
# 个股资金流	MoneyFlow	eastmoney,sina,joinquant
# 板块资金流	MoneyFlow	eastmoney,sina,joinquant	对应板块资料里面的标的
# 分红融资数据	DividendFinancing	eastmoney	企业最后的底线就是能不能给投资者赚钱,此为年度统计信息
# 分红明细	DividendDetail	eastmoney
# 融资明细	SPODetail	eastmoney
# 配股明细	RightsIssueDetail	eastmoney
# 主要财务指标	FinanceFactor	eastmoney
# 资产负债表	BalanceSheet	eastmoney
# 利润表	IncomeStatement	eastmoney
# 现金流量表	CashFlowStatement	eastmoney
# 十大股东	TopTenHolder	eastmoney
# 十大流通股东	TopTenTradableHolder	eastmoney
# 机构持股	InstitutionalInvestorHolder	eastmoney
# 高管交易	ManagerTrading	eastmoney
# 大股东交易	HolderTrading	eastmoney
# 大宗交易	BigDealTrading	eastmoney
# 融资融券	MarginTrading	eastmoney
# 龙虎榜数据	DragonAndTiger	eastmoney


@sched.scheduled_job('cron', hour=16, minute=0, day_of_week='mon-fri')
def record_day_kdata():
    while True:
        try:
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1DAY).run()

            break
        except Exception as e:
            logger.exception('joinquant_run_recorder joinquant day_kdata runner error:{}'.format(e))
            time.sleep(60*2)


# 每周6抓取周线和月线数据
@sched.scheduled_job('cron', day_of_week=5, hour=22, minute=30)
def record_wk_kdata():
    while True:
        try:
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1WEEK).run()
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1MON).run()

            break
        except Exception as e:
            logger.exception('joinquant_run_recorder joinquant wk_kdata runner error:{}'.format(e))
            time.sleep(60*2)


# 每周6抓取周线和月线数据
@sched.scheduled_job('cron', day_of_week=6, hour=22, minute=30)
def record_month_kdata():
    while True:
        try:
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1MON).run()

            break
        except Exception as e:
            logger.exception('joinquant_run_recorder joinquant month_kdata runner error:{}'.format(e))
            time.sleep(60*2)


if __name__ == '__main__':
    init_log('joinquant_run_recorder.log')

    record_day_kdata()
    record_wk_kdata()

    sched.start()

    sched._thread.join()
